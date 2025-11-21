package tests

import (
	"fmt"
	"grpc_client/pool"
)

var Pool *pool.Pool

func Reset() {
	Pool = nil
}

func GetPool(config *pool.PoolConfig) (*pool.Pool, func(), error) {

	// ctx := context.Background()

	target := "localhost:5050"

	fmt.Println("Initializing gRPC Pool")

	pool, err := pool.NewPool(target, config)

	if err != nil {
		fmt.Println("Error initializing grpc pool")
	}

	Pool = pool

	// go pool.ScheduledCleanup(ctx)

	return pool, Reset, nil
}
