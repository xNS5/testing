package main

import (
	"context"
	"fmt"
	logging "grpc-server/middleware/logging"
	hello "grpc-server/protobuf"
	"net"
	"time"

	"google.golang.org/grpc"
)

type HelloServer struct {
	hello.UnimplementedHelloServer
}

func (h *HelloServer) Hello(ctx context.Context, foo *hello.Request) (*hello.Response, error) {
	if foo.Timeout != nil && *foo.Timeout > 0 {
		time.Sleep(time.Duration(*foo.Timeout) * time.Second)
	}

	return &hello.Response{
		Res: "Hello, world!",
	}, nil
}

func main() {
	
	logging.Init()

	logging.Logger.Info().Msg("Initializing gRPC Server")
	serverAddr := fmt.Sprintf("localhost:%d", 5050)
	listen, err := net.Listen("tcp", serverAddr)


	if err != nil {
		logging.Logger.Error().Err(err).Msg("Error starting server")
		return
	}

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			logging.UnaryLoggingInterceptor(logging.Logger),
		),
	}

	server := grpc.NewServer(opts...)

	hello.RegisterHelloServer(server, &HelloServer{})

	logging.Logger.Info().Msgf("Serving gRPC server on %v\r\n", serverAddr)

	if err := server.Serve(listen); err != nil {
		logging.Logger.Error().Err(err).Msg("Error serving gRPC server")
	}

}
