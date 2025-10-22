package main

import (
	"context"
	"fmt"
	hello "grpc-server/protobuf"
	"net"
	"time"

	"google.golang.org/grpc"
)

type HelloServer struct {
	hello.UnimplementedHelloServer
}

func (h *HelloServer) Hello(ctx context.Context, foo *hello.Request) (*hello.Response, error) {
	time.Sleep(2 * time.Second)
	return &hello.Response{
		Res: "Hello, world!",
	}, nil
}

func main() {

	fmt.Println("Initializing gRPC Server")
	serverAddr := fmt.Sprintf("localhost:%d", 5050)
	listen, err := net.Listen("tcp", serverAddr)

	if err != nil {
		fmt.Printf("Error starting server")
		return
	}

	var opts []grpc.ServerOption

	server := grpc.NewServer(opts...)

	hello.RegisterHelloServer(server, &HelloServer{})

	fmt.Printf("Serving gRPC server on %v\r\n", serverAddr)

	if err := server.Serve(listen); err != nil {
		fmt.Printf("Error serving gRPC server")
	}

}
