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

	target := "localhost:5050"

	poolConfig := &pool.PoolConfig{
		MinConns:    1,
		MaxConns:    5,
		MaxPerConn:  2,
		Opts:        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		IdleTimeout: time.Duration(10 * time.Second),
		DialTimeout: time.Duration(10 * time.Second),
		ReqTimeout:  time.Duration(2 * time.Second),
	}

	fmt.Println("Initializing gRPC Pool")

	pool, err := pool.NewPool(target, poolConfig)

	if err != nil {
		fmt.Println("Error initializing grpc pool")
	}

	Pool = pool

	// go pool.ScheduledCleanup(ctx)

	return pool, Reset, nil
}
