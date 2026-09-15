package broker

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	pb "server-web/proto/remotedesktop"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ActiveClient struct {
	ClientID    string
	PIN         string
	MachineName string
	OSInfo      string
	RegisteredAt time.Time
	
	// Channel to send ControlMessages (input events) to the host
	HostControlChan chan *pb.ControlMessage
	
	// Subscribers (Control Viewers) receiving HostMessages (video frames)
	mu           sync.RWMutex
	subscribers  map[string]chan *pb.HostMessage
}

type Broker struct {
	pb.UnimplementedRemoteDesktopServer
	
	mu      sync.RWMutex
	clients map[string]*ActiveClient
}

func NewBroker() *Broker {
	return &Broker{
		clients: make(map[string]*ActiveClient),
	}
}

// GetActiveClients returns list of registered client hosts for Web UI
type ClientInfo struct {
	ClientID     string    `json:"client_id"`
	MachineName  string    `json:"machine_name"`
	OSInfo       string    `json:"os_info"`
	RegisteredAt time.Time `json:"registered_at"`
	HasControl   bool      `json:"has_control"`
}

func (b *Broker) ListClients() []ClientInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var list []ClientInfo
	for _, c := range b.clients {
		c.mu.RLock()
		hasControl := len(c.subscribers) > 0
		c.mu.RUnlock()

		list = append(list, ClientInfo{
			ClientID:     c.ClientID,
			MachineName:  c.MachineName,
			OSInfo:       c.OSInfo,
			RegisteredAt: c.RegisteredAt,
			HasControl:   hasControl,
		})
	}
	return list
}

func (b *Broker) RegisterClient(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if req.GetClientId() == "" || req.GetPin() == "" {
		return &pb.RegisterResponse{
			Success:      false,
			ErrorMessage: "Client ID and PIN are required",
		}, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	client := &ActiveClient{
		ClientID:        req.GetClientId(),
		PIN:             req.GetPin(),
		MachineName:     req.GetMachineName(),
		OSInfo:          req.GetOsInfo(),
		RegisteredAt:    time.Now(),
		HostControlChan: make(chan *pb.ControlMessage, 256),
		subscribers:     make(map[string]chan *pb.HostMessage),
	}

	b.clients[req.GetClientId()] = client
	log.Printf("[BROKER] Registered host client: %s (%s - %s)", req.GetClientId(), req.GetMachineName(), req.GetOsInfo())

	return &pb.RegisterResponse{
		Success:      true,
		SessionToken: fmt.Sprintf("tok_%s_%d", req.GetClientId(), time.Now().Unix()),
	}, nil
}

func (b *Broker) AuthenticateControl(ctx context.Context, req *pb.AuthRequest) (*pb.AuthResponse, error) {
	b.mu.RLock()
	client, exists := b.clients[req.GetTargetClientId()]
	b.mu.RUnlock()

	if !exists {
		return &pb.AuthResponse{
			Success:      false,
			ErrorMessage: "Target client host not found",
		}, nil
	}

	if client.PIN != req.GetPin() {
		return &pb.AuthResponse{
			Success:      false,
			ErrorMessage: "Invalid PIN code",
		}, nil
	}

	sessionID := fmt.Sprintf("sess_%s_%d", req.GetTargetClientId(), time.Now().UnixNano())
	log.Printf("[BROKER] Authenticated operator '%s' to client '%s' (Session: %s)", req.GetOperatorName(), req.GetTargetClientId(), sessionID)

	return &pb.AuthResponse{
		Success:   true,
		SessionId: sessionID,
	}, nil
}

func (b *Broker) HostStream(stream pb.RemoteDesktop_HostStreamServer) error {
	ctx := stream.Context()

	// Receive first message to identify host
	firstMsg, err := stream.Recv()
	if err != nil {
		return err
	}

	clientID := firstMsg.GetSessionId()
	b.mu.RLock()
	client, exists := b.clients[clientID]
	b.mu.RUnlock()

	if !exists {
		return status.Errorf(codes.NotFound, "Client ID %s not registered", clientID)
	}

	log.Printf("[BROKER] HostStream connected for Client ID: %s", clientID)

	// Goroutine to send control inputs received from operators to this Host agent
	errChan := make(chan error, 2)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ctrlMsg, ok := <-client.HostControlChan:
				if !ok {
					return
				}
				if err := stream.Send(ctrlMsg); err != nil {
					errChan <- err
					return
				}
			}
		}
	}()

	// Read loop: receive video frames and status from Host agent
	go func() {
		// Handle first message if it contains specs or status
		b.broadcastToSubscribers(client, firstMsg)

		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				errChan <- nil
				return
			}
			if err != nil {
				errChan <- err
				return
			}
			b.broadcastToSubscribers(client, msg)
		}
	}()

	select {
	case <-ctx.Done():
		log.Printf("[BROKER] HostStream closed for Client ID: %s", clientID)
		return ctx.Err()
	case err := <-errChan:
		log.Printf("[BROKER] HostStream ended for Client ID %s: %v", clientID, err)
		return err
	}
}

func (b *Broker) broadcastToSubscribers(client *ActiveClient, msg *pb.HostMessage) {
	client.mu.RLock()
	defer client.mu.RUnlock()

	for _, subChan := range client.subscribers {
		select {
		case subChan <- msg:
		default:
			// Drop frame if subscriber buffer is full to prevent backpressure blocking
		}
	}
}

func (b *Broker) ControlStream(stream pb.RemoteDesktop_ControlStreamServer) error {
	ctx := stream.Context()

	firstMsg, err := stream.Recv()
	if err != nil {
		return err
	}

	clientID := firstMsg.GetSessionId()
	b.mu.RLock()
	client, exists := b.clients[clientID]
	b.mu.RUnlock()

	if !exists {
		return status.Errorf(codes.NotFound, "Target Client ID %s not registered", clientID)
	}

	subID := fmt.Sprintf("sub_%d", time.Now().UnixNano())
	frameChan := make(chan *pb.HostMessage, 128)

	client.mu.Lock()
	client.subscribers[subID] = frameChan
	client.mu.Unlock()

	defer func() {
		client.mu.Lock()
		delete(client.subscribers, subID)
		client.mu.Unlock()
		close(frameChan)
		log.Printf("[BROKER] Operator disconnected from Client ID: %s", clientID)
	}()

	log.Printf("[BROKER] ControlStream connected to Client ID: %s (Sub: %s)", clientID, subID)

	errChan := make(chan error, 2)

	// Send video frames to Control Viewer
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case hostMsg, ok := <-frameChan:
				if !ok {
					return
				}
				if err := stream.Send(hostMsg); err != nil {
					errChan <- err
					return
				}
			}
		}
	}()

	// Receive input events from Control Viewer and forward to Host agent
	go func() {
		// Process first input message if any
		if firstMsg.GetInputEvent() != nil || firstMsg.GetCommand() != nil {
			client.HostControlChan <- firstMsg
		}

		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				errChan <- nil
				return
			}
			if err != nil {
				errChan <- err
				return
			}
			select {
			case client.HostControlChan <- msg:
			default:
				// Drop input if host queue full
			}
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}
