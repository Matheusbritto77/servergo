package main

import (
	"log"
	"net"
	"os"

	"server-web/broker"
	pb "server-web/proto/remotedesktop"
	"server-web/web"

	"google.golang.org/grpc"
)

func main() {
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}
	if grpcPort[0] != ':' {
		grpcPort = ":" + grpcPort
	}

	webPort := os.Getenv("WEB_PORT")
	if webPort == "" {
		webPort = "8090"
	}
	if webPort[0] != ':' {
		webPort = ":" + webPort
	}

	serverIP := os.Getenv("SERVER_IP")
	if serverIP == "" {
		serverIP = "209.126.81.68"
	}

	log.Printf("🌐 Server configured for Public IP: %s", serverIP)

	// 1. Initialize Broker
	b := broker.NewBroker()

	// 2. Start gRPC Server on 0.0.0.0:50051
	lis, err := net.Listen("tcp", "0.0.0.0"+grpcPort)
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port %s: %v", grpcPort, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRemoteDesktopServer(grpcServer, b)

	go func() {
		log.Printf("🚀 gRPC Broker listening on 0.0.0.0%s (Target Server IP: %s%s)", grpcPort, serverIP, grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	// 3. Start Web Dashboard HTTP Server on 0.0.0.0:8090
	webServer := web.NewWebServer(b)
	log.Printf("🌐 Web Dashboard listening on http://0.0.0.0%s (Target Server IP: http://%s%s)", webPort, serverIP, webPort)
	if err := webServer.Start("0.0.0.0" + webPort); err != nil {
		log.Fatalf("Web server error: %v", err)
	}
}
