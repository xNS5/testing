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

	target := "localhost:5050"

	poolConfig := &pool.PoolConfig{
		MinConns:    1,
		MaxConns:    5,
		Opts:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		IdleTimeout: time.Duration(10 * time.Second),
		DialTimeout: time.Duration(10 * time.Second),
		ReqTimeout:  time.Duration(10 * time.Second),
	}

	fmt.Println("Initializing gRPC Pool")

	pool, err := pool.NewPool(target, poolConfig)

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
