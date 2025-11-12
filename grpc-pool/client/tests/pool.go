package tests

import (
	"fmt"
	"grpc_client/pool"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var Pool *pool.Pool

func Reset() {
	Pool = nil
}

func GetPool() (*pool.Pool, func(), error) {

	// ctx := context.Background()

	if Pool != nil {
		return Pool, nil, nil
	}

	poolConfig := &pool.Pool{
		Target:     "localhost:5050",
		Timeout:    time.Duration(10 * time.Second),
		RPCTimeout: time.Duration(4 * time.Second),
		Opts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		MaxConns:   10,
		MaxPerConn: 2,
	}

	// fmt.Println("Initializing gRPC Pool")
	pool, err := pool.NewPool(poolConfig)

	if err != nil {
		fmt.Println("Error initializing grpc pool")
		return nil, nil, err
	}

	Pool = pool

	// go pool.ScheduledCleanup(ctx)

	return pool, Reset, nil
}
