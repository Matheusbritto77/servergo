package broker

/*
#cgo CFLAGS: -I../nexus-core
#include "nexus_core.h"
*/
import "C"

import (
	"fmt"
	"net"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const (
	nexusPeerTTL         = 2 * time.Minute
	nexusCleanupInterval = 30 * time.Second
	nexusSocketBuffer    = 16 * 1024 * 1024
	nexusDSCPLowLatency  = 0xb8 // Expedited Forwarding DSCP 46.
)

type nexusPeer struct {
	addr     *net.UDPAddr
	lastSeen time.Time
}

type NexusRelayServer struct {
	port       int
	conn       *net.UDPConn
	peersMutex sync.RWMutex
	peerMap    map[uint32]nexusPeer
}

func NewNexusRelayServer(port int) *NexusRelayServer {
	return &NexusRelayServer{
		port:    port,
		peerMap: make(map[uint32]nexusPeer),
	}
}

func (s *NexusRelayServer) Start() error {
	addr := net.UDPAddr{
		Port: s.port,
		IP:   net.ParseIP("0.0.0.0"),
	}

	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		return fmt.Errorf("nexus udp listen error: %w", err)
	}
	tuneNexusSocket(conn)
	s.conn = conn

	fmt.Printf("[NEXUS-P2P ENGINE] ⚡ Ultra-Low Latency Protocol Server Running on UDP 0.0.0.0:%d\n", s.port)

	go s.cleanupLoop()
	go s.listenLoop()
	return nil
}

func (s *NexusRelayServer) listenLoop() {
	buf := make([]byte, 512*1024) // 512 KB zero-alloc frame buffer

	for {
		n, remoteAddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			break
		}

		if n < int(C.NEXUS_CORE_HEADER_LEN) {
			continue
		}

		hdr, err := decodeNexusHeader(buf[:n])
		if err != nil {
			continue
		}

		// Register or Update Peer STUN/NAT Reflect Endpoint
		s.peersMutex.Lock()
		s.peerMap[uint32(hdr.stream_id)] = nexusPeer{
			addr:     remoteAddr,
			lastSeen: time.Now(),
		}
		s.peersMutex.Unlock()

		switch hdr.frame_type {
		case C.NEXUS_CORE_TYPE_KNOCK:
			// NAT Hole Punching Knock Probe - Echo back reflected endpoint
			resp := make([]byte, int(C.NEXUS_CORE_HEADER_LEN))
			respLen := C.nexus_core_pack_frame(
				C.NEXUS_CORE_TYPE_KNOCK,
				hdr.stream_id,
				hdr.seq_num,
				hdr.seq_num,
				0,
				0,
				nil,
				0,
				(*C.uint8_t)(unsafe.Pointer(&resp[0])),
				C.size_t(len(resp)),
			)
			if respLen > 0 {
				_, _ = s.conn.WriteToUDP(resp[:int(respLen)], remoteAddr)
			}

		case C.NEXUS_CORE_TYPE_VIDEO, C.NEXUS_CORE_TYPE_INPUT, C.NEXUS_CORE_TYPE_SIGNAL,
			C.NEXUS_CORE_TYPE_SYNC_KEYFRAME, C.NEXUS_CORE_TYPE_KEEPALIVE,
			C.NEXUS_CORE_TYPE_ACK, C.NEXUS_CORE_TYPE_NACK,
			C.NEXUS_CORE_TYPE_PATH_CHALLENGE, C.NEXUS_CORE_TYPE_PATH_RESPONSE:
			// Low-latency O(1) datagram forwarding to paired peer endpoint (StreamID ^ 1)
			targetStreamID := uint32(hdr.stream_id) ^ 1
			s.peersMutex.RLock()
			targetPeer, exists := s.peerMap[targetStreamID]
			s.peersMutex.RUnlock()
			if exists && targetPeer.addr != nil {
				_, _ = s.conn.WriteToUDP(buf[:n], targetPeer.addr)
			}
		}
	}
}

func (s *NexusRelayServer) cleanupLoop() {
	ticker := time.NewTicker(nexusCleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		cutoff := time.Now().Add(-nexusPeerTTL)
		s.peersMutex.Lock()
		for streamID, peer := range s.peerMap {
			if peer.lastSeen.Before(cutoff) {
				delete(s.peerMap, streamID)
			}
		}
		s.peersMutex.Unlock()
	}
}

func tuneNexusSocket(conn *net.UDPConn) {
	_ = conn.SetReadBuffer(nexusSocketBuffer)
	_ = conn.SetWriteBuffer(nexusSocketBuffer)

	rawConn, err := conn.SyscallConn()
	if err != nil {
		return
	}
	_ = rawConn.Control(func(fd uintptr) {
		_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_TOS, nexusDSCPLowLatency)
	})
}

func decodeNexusHeader(data []byte) (C.nexus_core_header, error) {
	var hdr C.nexus_core_header
	if len(data) == 0 {
		return hdr, fmt.Errorf("empty nexus packet")
	}
	rc := C.nexus_core_decode(
		(*C.uint8_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		&hdr,
	)
	if rc != 0 {
		return hdr, fmt.Errorf("nexus decode failed: %d", int(rc))
	}
	return hdr, nil
}
