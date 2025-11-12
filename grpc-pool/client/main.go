package main

import (
	"context"
	"fmt"
	"grpc_client/pool"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	poolConfig := &pool.Pool{
		Target:     "localhost:5050",
		Timeout:    time.Duration(30 * time.Second),
		Opts:       []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		MaxConns:   2,
		MaxPerConn: 5,
	}

	fmt.Println("Initializing gRPC Pool")
	pool, err := pool.NewPool(poolConfig)

	if err != nil {
		fmt.Println("Error initializing grpc pool")
	}

	ctx := context.Background()

	fmt.Println("Getting conn")

	conn, err := pool.Get(ctx)

	if err != nil {
		fmt.Println("Error initializing grpc pool")
	}

	defer conn.Close()

	fmt.Println("Conn Success")
}
