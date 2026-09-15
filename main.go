package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"server-web/broker"
	pb "server-web/proto/remotedesktop"
	"server-web/web"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func main() {
	rawGrpcPort := os.Getenv("GRPC_PORT")
	if rawGrpcPort == "" {
		rawGrpcPort = "50051"
	}
	grpcPort := strings.TrimPrefix(rawGrpcPort, ":")
	grpcAddr := fmt.Sprintf("0.0.0.0:%s", grpcPort)

	// PaaS platforms like Coolify / Heroku pass the main web HTTP port via PORT env var
	rawWebPort := os.Getenv("PORT")
	if rawWebPort == "" {
		rawWebPort = os.Getenv("WEB_PORT")
	}
	if rawWebPort == "" {
		rawWebPort = "8090"
	}
	webPort := strings.TrimPrefix(rawWebPort, ":")
	webAddr := fmt.Sprintf("0.0.0.0:%s", webPort)

	serverIP := os.Getenv("SERVER_IP")
	if serverIP == "" {
		serverIP = "209.126.81.68"
	}

	log.Printf("🌐 Server configured for Public IP: %s", serverIP)

	// 1. Initialize Broker
	b := broker.NewBroker()

	// 2. Start gRPC Server on 0.0.0.0:50051 with High-Throughput Keepalive & Window Tuning
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("Failed to listen on gRPC address %s: %v", grpcAddr, err)
	}

	kaParams := keepalive.ServerParameters{
		MaxConnectionIdle: 15 * time.Minute,
		Time:              10 * time.Second,
		Timeout:           3 * time.Second,
	}

	kaEnforce := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(kaParams),
		grpc.KeepaliveEnforcementPolicy(kaEnforce),
		grpc.MaxRecvMsgSize(32*1024*1024),
		grpc.MaxSendMsgSize(32*1024*1024),
		grpc.InitialWindowSize(4*1024*1024),
		grpc.InitialConnWindowSize(8*1024*1024),
	)
	pb.RegisterRemoteDesktopServer(grpcServer, b)

	go func() {
		log.Printf("🚀 High-Throughput gRPC Broker listening on ALL INTERFACES (%s) [Target Server IP: %s:%s]", grpcAddr, serverIP, grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// 2.5 Start NEXUS-P2P Custom Protocol Relay Engine on UDP 0.0.0.0:50052
	go func() {
		nexusServer := broker.NewNexusRelayServer(50052)
		if err := nexusServer.Start(); err != nil {
			log.Printf("[NEXUS] Failed to start NEXUS UDP server: %v", err)
		}
	}()

	// 3. Start Web Dashboard HTTP Server on 0.0.0.0:8090 (or PORT env)
	webServer := web.NewWebServer(b)
	log.Printf("🌐 Web Dashboard listening on ALL INTERFACES (http://%s) [Target Server IP: http://%s:%s]", webAddr, serverIP, webPort)
	if err := webServer.Start(webAddr); err != nil {
		log.Fatalf("Web server error: %v", err)
	}
}
