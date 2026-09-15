package broker

/*
#cgo CFLAGS: -I../nexus-core
#include "nexus_core.h"
*/
import "C"

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
	"unsafe"

	pb "server-web/proto/remotedesktop"

	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

// NexusControlTag identifies the type of protobuf payload inside a Nexus Control frame body.
const (
	NexusControlTagCommand byte = 0x01
	NexusControlTagStatus  byte = 0x02
)

type ActiveClient struct {
	ClientID     string
	MachineName  string
	OSInfo       string
	RemoteIP     string
	RegisteredAt time.Time
}

type Broker struct {
	pb.UnimplementedRemoteDesktopServer

	mu      sync.RWMutex
	clients map[string]*ActiveClient
	rnd     *rand.Rand

	// Reference to the Nexus relay for sending control frames to peers.
	nexusRelay *NexusRelayServer
}

func NewBroker() *Broker {
	return &Broker{
		clients: make(map[string]*ActiveClient),
		rnd:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SetNexusRelay connects the broker to the Nexus UDP relay so it can
// dispatch control frames (e.g. ConnectRequest) to host peers.
func (b *Broker) SetNexusRelay(relay *NexusRelayServer) {
	b.nexusRelay = relay
}

func normalizeID(id string) string {
	return strings.ReplaceAll(strings.ReplaceAll(id, "-", ""), " ", "")
}

func (b *Broker) findClient(rawID string) (*ActiveClient, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	cleanTarget := normalizeID(rawID)
	for _, c := range b.clients {
		if normalizeID(c.ClientID) == cleanTarget {
			return c, true
		}
	}
	return nil, false
}

func (b *Broker) generateUniqueID() string {
	for {
		id := fmt.Sprintf("%03d-%03d-%03d", b.rnd.Intn(900)+100, b.rnd.Intn(900)+100, b.rnd.Intn(900)+100)
		if _, exists := b.clients[id]; !exists {
			return id
		}
	}
}

type ClientInfo struct {
	ClientID     string    `json:"client_id"`
	MachineName  string    `json:"machine_name"`
	OSInfo       string    `json:"os_info"`
	RegisteredAt time.Time `json:"registered_at"`
}

func (b *Broker) ListClients() []ClientInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var list []ClientInfo
	for _, c := range b.clients {
		list = append(list, ClientInfo{
			ClientID:     c.ClientID,
			MachineName:  c.MachineName,
			OSInfo:       c.OSInfo,
			RegisteredAt: c.RegisteredAt,
		})
	}
	return list
}

func (b *Broker) CheckUpdate(ctx context.Context, req *pb.UpdateCheckRequest) (*pb.UpdateCheckResponse, error) {
	latestVersion := "0.1.0"
	downloadURL := fmt.Sprintf("http://209.126.81.68:8080/downloads/remote-%s", req.GetComponent())
	if req.GetOsTarget() == "windows" {
		downloadURL += ".exe"
	}

	updateAvailable := req.GetCurrentVersion() != "" && req.GetCurrentVersion() != latestVersion

	log.Printf("🔄 [AUTO-UPDATE CHECK] Component: %s (%s), Client Version: %s, Server Version: %s", req.GetComponent(), req.GetOsTarget(), req.GetCurrentVersion(), latestVersion)

	return &pb.UpdateCheckResponse{
		UpdateAvailable: updateAvailable,
		LatestVersion:   latestVersion,
		DownloadUrl:     downloadURL,
	}, nil
}

func (b *Broker) RegisterClient(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	clientID := b.generateUniqueID()

	var remoteIP string
	if pr, ok := peer.FromContext(ctx); ok && pr.Addr != nil {
		host, _, err := net.SplitHostPort(pr.Addr.String())
		if err == nil {
			remoteIP = host
		} else {
			remoteIP = pr.Addr.String()
		}
	}

	client := &ActiveClient{
		ClientID:     clientID,
		MachineName:  req.GetMachineName(),
		OSInfo:       req.GetOsInfo(),
		RemoteIP:     remoteIP,
		RegisteredAt: time.Now(),
	}

	b.clients[clientID] = client
	log.Printf("⚡ [SERVER ID GENERATOR] Generated & Registered Client ID: %s for host (%s - %s - IP: %s)", clientID, req.GetMachineName(), req.GetOsInfo(), remoteIP)

	return &pb.RegisterResponse{
		Success:      true,
		ClientId:     clientID,
		SessionToken: fmt.Sprintf("tok_%s_%d", clientID, time.Now().Unix()),
	}, nil
}

func (b *Broker) AuthenticateControl(ctx context.Context, req *pb.AuthRequest) (*pb.AuthResponse, error) {
	client, exists := b.findClient(req.GetTargetClientId())

	if !exists || client == nil {
		return &pb.AuthResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("Target Client ID %s is offline or not found", req.GetTargetClientId()),
		}, nil
	}

	sessionID := fmt.Sprintf("sess_%s_%d", client.ClientID, time.Now().UnixNano())
	p2pEndpoint := fmt.Sprintf("%s:50052", client.RemoteIP)
	log.Printf("🔗 [BROKER] Direct connection request from operator '%s' to Client ID '%s' (IP: %s - P2P Target: %s)", req.GetOperatorName(), client.ClientID, client.RemoteIP, p2pEndpoint)

	// Send ConnectRequest to the host via Nexus Control frame through the UDP relay.
	if b.nexusRelay != nil {
		cmd := &pb.ControlCommand{
			Type:   pb.CommandType_CONNECT_REQUEST,
			Detail: req.GetOperatorName(),
		}
		cmdBytes, err := proto.Marshal(cmd)
		if err == nil {
			// Nexus Control frame body: [1-byte tag][protobuf payload]
			body := make([]byte, 1+len(cmdBytes))
			body[0] = NexusControlTagCommand
			copy(body[1:], cmdBytes)

			// Build a Nexus Control frame targeting the host's stream_id.
			hostStreamID := nexusCoreSessionStreamID(client.ClientID, false)
			frame := make([]byte, int(C.NEXUS_CORE_HEADER_LEN)+len(body))
			frameLen := C.nexus_core_pack_frame(
				C.NEXUS_CORE_KIND_CONTROL,
				C.uint32_t(hostStreamID),
				0,
				0,
				0,
				0,
				(*C.uint8_t)(unsafe.Pointer(&body[0])),
				C.size_t(len(body)),
				(*C.uint8_t)(unsafe.Pointer(&frame[0])),
				C.size_t(len(frame)),
			)
			if frameLen > 0 {
				if b.nexusRelay.SendToStream(hostStreamID, frame[:int(frameLen)]) {
					log.Printf("🔔 [BROKER] ConnectRequest dispatched to Client ID '%s' via Nexus Control frame", client.ClientID)
				} else {
					log.Printf("⚠️ [BROKER] Host '%s' not reachable via Nexus relay (no UDP route registered)", client.ClientID)
				}
			}
		}
	}

	return &pb.AuthResponse{
		Success:     true,
		SessionId:   sessionID,
		P2PEndpoint: p2pEndpoint,
	}, nil
}

// nexusCoreSessionStreamID computes the Nexus stream_id for a given client ID string.
func nexusCoreSessionStreamID(clientID string, isOperator bool) uint32 {
	data := []byte(clientID)
	isOp := C.int(0)
	if isOperator {
		isOp = 1
	}
	return uint32(C.nexus_core_session_stream_id(
		(*C.uint8_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		isOp,
	))
}
