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
	"unsafe"
)

type NexusRelayServer struct {
	port       int
	conn       *net.UDPConn
	peersMutex sync.RWMutex
	peerMap    map[uint32]*net.UDPAddr
}

func NewNexusRelayServer(port int) *NexusRelayServer {
	return &NexusRelayServer{
		port:    port,
		peerMap: make(map[uint32]*net.UDPAddr),
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
	_ = conn.SetReadBuffer(4 * 1024 * 1024)
	_ = conn.SetWriteBuffer(4 * 1024 * 1024)
	s.conn = conn

	fmt.Printf("[NEXUS-P2P ENGINE] ⚡ Ultra-Low Latency Protocol Server Running on UDP 0.0.0.0:%d\n", s.port)

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
		s.peerMap[uint32(hdr.stream_id)] = remoteAddr
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
			targetAddr, exists := s.peerMap[targetStreamID]
			s.peersMutex.RUnlock()
			if exists && targetAddr != nil {
				_, _ = s.conn.WriteToUDP(buf[:n], targetAddr)
			}
		}
	}
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
