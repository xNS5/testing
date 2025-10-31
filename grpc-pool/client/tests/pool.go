package tests

import (
	"context"
	"fmt"
	"grpc_client/grpc_pool"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var Pool *grpc_pool.Pool

func Reset() {
	Pool = nil
}

func GetPool() (*grpc_pool.Pool, func(), error) {

	if Pool != nil {
		return Pool, nil, nil
	}

	poolConfig := &grpc_pool.Pool{
		Target:     "localhost:5050",
		Timeout:    time.Duration(10 * time.Second),
		RPCTimeout: time.Duration(10 * time.Second),
		Opts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		MaxConns:   4,
		MaxPerConn: 2,
	}

	// fmt.Println("Initializing gRPC Pool")
	pool, err := grpc_pool.NewPool(poolConfig)

	if err != nil {
		fmt.Println("Error initializing grpc pool")
		return nil, nil, err
	}

	Pool = pool

	pool.ScheduledCleanup(context.Background())

	return pool, Reset, nil
}
