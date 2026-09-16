package broker

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	pb "server-web/proto/remotedesktop"

	"google.golang.org/grpc/peer"
)

type ActiveClient struct {
	ClientID     string
	MachineName  string
	OSInfo       string
	RemoteIP     string
	RegisteredAt time.Time

	// gRPC 6-Channel Stream Handlers
	videoSubscribers   map[chan *pb.VideoFrame]bool
	videoSubMutex      sync.RWMutex
	videoControlChan   chan *pb.VideoControlCommand

	inputSubscribers   map[chan *pb.InputEvent]bool
	inputSubMutex      sync.RWMutex
	inputAckChan       chan *pb.InputAck

	controlSubscribers map[chan *pb.ControlMessage]bool
	controlSubMutex    sync.RWMutex
}

type Broker struct {
	pb.UnimplementedRemoteDesktopServer
	mu      sync.RWMutex
	clients map[string]*ActiveClient
	rnd     *rand.Rand
}

func NewBroker() *Broker {
	return &Broker{
		clients: make(map[string]*ActiveClient),
		rnd:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
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

func (b *Broker) normalizeClientID(id string) string {
	if strings.HasPrefix(id, "sess_") {
		parts := strings.Split(id, "_")
		if len(parts) >= 2 {
			return parts[1]
		}
	}
	return id
}

func (b *Broker) findClient(clientID string) (*ActiveClient, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	id := b.normalizeClientID(clientID)
	c, exists := b.clients[id]
	return c, exists
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
		ClientID:           clientID,
		MachineName:        req.GetMachineName(),
		OSInfo:             req.GetOsInfo(),
		RemoteIP:           remoteIP,
		RegisteredAt:       time.Now(),
		videoSubscribers:   make(map[chan *pb.VideoFrame]bool),
		videoControlChan:   make(chan *pb.VideoControlCommand, 128),
		inputSubscribers:   make(map[chan *pb.InputEvent]bool),
		inputAckChan:       make(chan *pb.InputAck, 128),
		controlSubscribers: make(map[chan *pb.ControlMessage]bool),
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
	log.Printf("🔗 [BROKER] Direct connection request from operator '%s' to Client ID '%s' (IP: %s)", req.GetOperatorName(), client.ClientID, client.RemoteIP)

	// Dispatch ConnectRequest to Host via ControlCommandStream
	msg := &pb.ControlMessage{
		SessionId: sessionID,
		Payload: &pb.ControlMessage_Command{
			Command: &pb.ControlCommand{
				Type:   pb.CommandType_CONNECT_REQUEST,
				Detail: req.GetOperatorName(),
			},
		},
	}
	b.BroadcastControlMessage(client.ClientID, msg)

	return &pb.AuthResponse{
		Success:   true,
		SessionId: sessionID,
	}, nil
}

// ── 6-Channel gRPC Streaming RPC Handlers ──

// Channel 1 & 2: VideoStream (Host streams VideoFrame -> Broker relays to Operators; Operators stream VideoControlCommand -> Broker relays to Host)
func (b *Broker) VideoStream(stream pb.RemoteDesktop_VideoStreamServer) error {
	var registeredID string

	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if registeredID == "" {
			// Find active client registered
			registeredID = b.getFirstClientID()
		}

		if registeredID != "" {
			b.BroadcastVideoFrame(registeredID, frame)
		}
	}
}

func (b *Broker) getFirstClientID() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for id := range b.clients {
		return id
	}
	return ""
}

func (b *Broker) BroadcastVideoFrame(clientID string, frame *pb.VideoFrame) {
	client, exists := b.findClient(clientID)
	if !exists || client == nil {
		return
	}

	client.videoSubMutex.RLock()
	defer client.videoSubMutex.RUnlock()

	for ch := range client.videoSubscribers {
		select {
		case ch <- frame:
		default:
			// Non-blocking write: drop buffer overflow frames to guarantee 0 lag
		}
	}
}

// Channel 3 & 4: InputStream (Operators send InputEvent -> Broker forwards to Host)
func (b *Broker) InputStream(stream pb.RemoteDesktop_InputStreamServer) error {
	var targetID string
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if targetID == "" {
			targetID = b.getFirstClientID()
		}

		if targetID != "" {
			b.BroadcastInputEvent(targetID, event)
		}
	}
}

func (b *Broker) BroadcastInputEvent(clientID string, event *pb.InputEvent) {
	client, exists := b.findClient(clientID)
	if !exists || client == nil {
		return
	}

	client.inputSubMutex.RLock()
	defer client.inputSubMutex.RUnlock()

	for ch := range client.inputSubscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

// Channel 5 & 6: ControlCommandStream (Bidirectional control messages between Host, Broker, and Operator)
func (b *Broker) ControlCommandStream(stream pb.RemoteDesktop_ControlCommandStreamServer) error {
	var clientID string
	var ch chan *pb.ControlMessage
	done := make(chan struct{})

	defer func() {
		close(done)
		if clientID != "" && ch != nil {
			if client, exists := b.findClient(clientID); exists {
				client.controlSubMutex.Lock()
				delete(client.controlSubscribers, ch)
				client.controlSubMutex.Unlock()
				close(ch)
			}
		}
	}()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if clientID == "" && msg.SessionId != "" {
			clientID = b.normalizeClientID(msg.SessionId)
			ch = make(chan *pb.ControlMessage, 128)
			if client, exists := b.findClient(clientID); exists {
				client.controlSubMutex.Lock()
				client.controlSubscribers[ch] = true
				client.controlSubMutex.Unlock()

				go func(subscriberChan chan *pb.ControlMessage) {
					for {
						select {
						case <-done:
							return
						case outMsg, ok := <-subscriberChan:
							if !ok {
								return
							}
							if err := stream.Send(outMsg); err != nil {
								return
							}
						}
					}
				}(ch)
			}
		}

		if clientID != "" {
			b.BroadcastControlMessage(clientID, msg)
		}
	}
}

func (b *Broker) BroadcastControlMessage(clientID string, msg *pb.ControlMessage) {
	client, exists := b.findClient(clientID)
	if !exists || client == nil {
		return
	}

	client.controlSubMutex.RLock()
	defer client.controlSubMutex.RUnlock()

	for ch := range client.controlSubscribers {
		select {
		case ch <- msg:
		default:
		}
	}
}
