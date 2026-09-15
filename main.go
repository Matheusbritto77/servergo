package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"server-web/broker"
	pb "server-web/proto/remotedesktop"
	"server-web/web"

	"google.golang.org/grpc"
)

func main() {
	rawGrpcPort := os.Getenv("GRPC_PORT")
	if rawGrpcPort == "" {
		rawGrpcPort = "50051"
	}
	grpcPort := strings.TrimPrefix(rawGrpcPort, ":")
	grpcAddr := fmt.Sprintf("0.0.0.0:%s", grpcPort)

	rawWebPort := os.Getenv("WEB_PORT")
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

	// 2. Start gRPC Server explicitly on 0.0.0.0:50051 (all network interfaces)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("Failed to listen on gRPC address %s: %v", grpcAddr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRemoteDesktopServer(grpcServer, b)

	go func() {
		log.Printf("🚀 gRPC Broker listening on ALL INTERFACES (%s) [Target Server IP: %s:%s]", grpcAddr, serverIP, grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// 3. Start Web Dashboard HTTP Server explicitly on 0.0.0.0:8090 (all network interfaces)
	webServer := web.NewWebServer(b)
	log.Printf("🌐 Web Dashboard listening on ALL INTERFACES (http://%s) [Target Server IP: http://%s:%s]", webAddr, serverIP, webPort)
	if err := webServer.Start(webAddr); err != nil {
		log.Fatalf("Web server error: %v", err)
	}
}
