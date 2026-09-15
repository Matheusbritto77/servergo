package broker

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
)

const (
	NexusMagic      = "NX"
	NexusVersion    = 0x01
	NexusHeaderLen  = 16
	NexusTypeKnock  = 0x01
	NexusTypeSignal = 0x02
	NexusTypeVideo  = 0x03
	NexusTypeInput  = 0x04
	NexusTypeKeep   = 0x05
)

type NexusHeader struct {
	Magic      [2]byte
	Version    uint8
	FrameType  uint8
	StreamID   uint32
	SeqNum     uint32
	PayloadLen uint32
}

func DecodeNexusHeader(data []byte) (*NexusHeader, error) {
	if len(data) < NexusHeaderLen {
		return nil, fmt.Errorf("header too short")
	}
	if data[0] != 'N' || data[1] != 'X' {
		return nil, fmt.Errorf("invalid magic signature")
	}
	return &NexusHeader{
		Magic:      [2]byte{data[0], data[1]},
		Version:    data[2],
		FrameType:  data[3],
		StreamID:   binary.BigEndian.Uint32(data[4:8]),
		SeqNum:     binary.BigEndian.Uint32(data[8:12]),
		PayloadLen: binary.BigEndian.Uint32(data[12:16]),
	}, nil
}

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

		if n < NexusHeaderLen {
			continue
		}

		hdr, err := DecodeNexusHeader(buf[:n])
		if err != nil {
			continue
		}

		// Register or Update Peer STUN/NAT Reflect Endpoint
		s.peersMutex.Lock()
		s.peerMap[hdr.StreamID] = remoteAddr
		s.peersMutex.Unlock()

		switch hdr.FrameType {
		case NexusTypeKnock:
			// NAT Hole Punching Knock Probe - Echo back reflected endpoint
			respHdr := make([]byte, NexusHeaderLen)
			respHdr[0], respHdr[1] = 'N', 'X'
			respHdr[2] = NexusVersion
			respHdr[3] = NexusTypeKnock
			binary.BigEndian.PutUint32(respHdr[4:8], hdr.StreamID)
			s.conn.WriteToUDP(respHdr, remoteAddr)

		case NexusTypeVideo, NexusTypeInput, NexusTypeSignal:
			// Low-latency datagram forwarding to paired peer endpoint
			s.peersMutex.RLock()
			for streamID, targetAddr := range s.peerMap {
				if streamID != hdr.StreamID {
					s.conn.WriteToUDP(buf[:n], targetAddr)
				}
			}
			s.peersMutex.RUnlock()
		}
	}
}
