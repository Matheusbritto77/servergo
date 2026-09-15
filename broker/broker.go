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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type ActiveClient struct {
	ClientID     string
	MachineName  string
	OSInfo       string
	RemoteIP     string
	RegisteredAt time.Time

	HostControlChan chan *pb.ControlMessage
	mu              sync.RWMutex
	subscribers     map[string]chan *pb.HostMessage
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
		ClientID:        clientID,
		MachineName:     req.GetMachineName(),
		OSInfo:          req.GetOsInfo(),
		RemoteIP:        remoteIP,
		RegisteredAt:    time.Now(),
		HostControlChan: make(chan *pb.ControlMessage, 512),
		subscribers:     make(map[string]chan *pb.HostMessage),
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

	// Send CONNECT_REQUEST command to host agent
	select {
	case client.HostControlChan <- &pb.ControlMessage{
		SessionId: sessionID,
		Payload: &pb.ControlMessage_Command{
			Command: &pb.ControlCommand{
				Type:   pb.CommandType_CONNECT_REQUEST,
				Detail: req.GetOperatorName(),
			},
		},
	}:
		log.Printf("🔔 [BROKER] Connection request dispatched to Client ID '%s'", client.ClientID)
	default:
		log.Printf("⚠️ [BROKER] Could not deliver connection request to Client ID '%s' (buffer full)", client.ClientID)
	}

	return &pb.AuthResponse{
		Success:     true,
		SessionId:   sessionID,
		P2PEndpoint: p2pEndpoint,
	}, nil
}

func (b *Broker) HostStream(stream pb.RemoteDesktop_HostStreamServer) error {
	ctx := stream.Context()

	firstMsg, err := stream.Recv()
	if err != nil {
		return err
	}

	clientID := firstMsg.GetSessionId()
	client, exists := b.findClient(clientID)

	if !exists {
		return status.Errorf(codes.NotFound, "Client ID %s not registered", clientID)
	}

	log.Printf("[BROKER] HostStream connected for Client ID: %s", client.ClientID)

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

	go func() {
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
			// Non-blocking zero-lag strategy: drop oldest frame and insert latest frame immediately
			select {
			case <-subChan:
			default:
			}
			select {
			case subChan <- msg:
			default:
			}
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
	client, exists := b.findClient(clientID)

	if !exists {
		return status.Errorf(codes.NotFound, "Target Client ID %s not registered", clientID)
	}

	subID := fmt.Sprintf("sub_%d", time.Now().UnixNano())
	frameChan := make(chan *pb.HostMessage, 4)

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

	go func() {
		if firstMsg.GetInputEvent() != nil || firstMsg.GetCommand() != nil || firstMsg.GetWebrtcSignal() != nil {
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
