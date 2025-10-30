package main

import (
	"context"
	"fmt"
	cancellationhandler "grpc-server/middleware/cancellation-handler"
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
		logging.Logger.Debug().Msgf("Sleeping for %v seconds", *foo.Timeout)
		for range *foo.Timeout {
			time.Sleep(1 * time.Second)
		}
	}

	return &hello.Response{
		Res: "Hello, world!",
	}, nil
}

func main() {

	logging.Init()

	log := logging.Logger

	log.Info().Msg("Initializing gRPC Server")
	serverAddr := fmt.Sprintf("localhost:%d", 5050)
	listen, err := net.Listen("tcp", serverAddr)

	if err != nil {
		log.Error().Err(err).Msg("Error starting server")
		return
	}

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			logging.UnaryLoggingInterceptor(log),
			cancellationhandler.UnaryCancellationInterceptor(),
		),
	}

	server := grpc.NewServer(opts...)

	hello.RegisterHelloServer(server, &HelloServer{})

	log.Info().Msgf("Serving gRPC server on %v", serverAddr)

	if err := server.Serve(listen); err != nil {
		log.Error().Err(err).Msg("Error serving gRPC server")
	}
}
