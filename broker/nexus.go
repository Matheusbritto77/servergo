package broker

/*
#cgo CFLAGS: -I../nexus-core
#include "nexus_core.h"
*/
import "C"

import (
	"encoding/binary"
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
		return fmt.Errorf("nexus carrier listen error: %w", err)
	}
	tuneNexusSocket(conn)
	s.conn = conn

	fmt.Printf("[NEXUS MESH] ⚡ Low-latency carrier listening on 0.0.0.0:%d\n", s.port)

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

		// Register or update the broker-observed route endpoint (Zero-Disconnection IP Roaming).
		s.peersMutex.Lock()
		oldPeer, exists := s.peerMap[uint32(hdr.stream_id)]
		if exists && oldPeer.addr != nil && oldPeer.addr.String() != remoteAddr.String() {
			fmt.Printf("[NEXUS ROAMING] 🔄 Dynamic IP Roaming detected for stream_id 0x%x -> %s\n", hdr.stream_id, remoteAddr)
		}
		s.peerMap[uint32(hdr.stream_id)] = nexusPeer{
			addr:     remoteAddr,
			lastSeen: time.Now(),
		}
		s.peersMutex.Unlock()

		if hdr.kind == C.NEXUS_CORE_KIND_TRACE {
			// STUN-like reflexive endpoint discovery: return client's public IP & Port.
			ip4 := remoteAddr.IP.To4()
			var ipUint uint32
			if ip4 != nil {
				ipUint = binary.BigEndian.Uint32(ip4)
			}
			resp := make([]byte, int(C.NEXUS_CORE_HEADER_LEN)+6)
			respLen := C.nexus_core_pack_trace_reply(
				hdr.stream_id,
				hdr.seq_num,
				C.uint32_t(ipUint),
				C.uint16_t(remoteAddr.Port),
				(*C.uint8_t)(unsafe.Pointer(&resp[0])),
				C.size_t(len(resp)),
			)
			if respLen > 0 {
				_, _ = s.conn.WriteToUDP(resp[:int(respLen)], remoteAddr)
			}
		}

		switch C.nexus_core_route_action(hdr.kind) {
		case C.NEXUS_CORE_ROUTE_HELLO_REPLY:
			// Reply with the broker-observed path so peers can converge on a route.
			resp := make([]byte, int(C.NEXUS_CORE_HEADER_LEN))
			respLen := C.nexus_core_pack_hello_reply(
				&hdr,
				(*C.uint8_t)(unsafe.Pointer(&resp[0])),
				C.size_t(len(resp)),
			)
			if respLen > 0 {
				_, _ = s.conn.WriteToUDP(resp[:int(respLen)], remoteAddr)
			}

		case C.NEXUS_CORE_ROUTE_RELAY:
			// Low-latency O(1) frame forwarding to the paired stream.
			targetStreamID := uint32(C.nexus_core_pair_stream_id(hdr.stream_id))
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

// RegisterPeer pre-registers or updates a stream_id -> UDP address route in the peer map.
func (s *NexusRelayServer) RegisterPeer(streamID uint32, addr *net.UDPAddr) {
	if addr == nil {
		return
	}
	s.peersMutex.Lock()
	defer s.peersMutex.Unlock()
	s.peerMap[streamID] = nexusPeer{
		addr:     addr,
		lastSeen: time.Now(),
	}
}

// SendToStream sends a raw Nexus frame to the peer registered under the given stream_id.
// Returns true if the peer was found and the frame was dispatched.
func (s *NexusRelayServer) SendToStream(streamID uint32, data []byte) bool {
	if s.conn == nil || len(data) == 0 {
		return false
	}
	s.peersMutex.RLock()
	peer, exists := s.peerMap[streamID]
	s.peersMutex.RUnlock()
	if !exists || peer.addr == nil {
		return false
	}
	_, err := s.conn.WriteToUDP(data, peer.addr)
	return err == nil
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
		return hdr, fmt.Errorf("empty nexus frame")
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
