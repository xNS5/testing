package tests

import (
	"fmt"
	"grpc_client/grpc_pool"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var Pool *grpc_pool.Pool

func GetPool() (*grpc_pool.Pool, error) {

	if Pool != nil {
		return Pool, nil
	}

	poolConfig := &grpc_pool.Pool{
		Target:     "localhost:5050",
		Timeout:    time.Duration(30 * time.Second),
		Opts:       []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		MaxConns:   2,
		MaxPerConn: 5,
	}

	fmt.Println("Initializing gRPC Pool")
	pool, err := grpc_pool.NewPool(poolConfig)

	if err != nil {
		fmt.Println("Error initializing grpc pool")
		return nil, err
	}

	Pool = pool

	return pool, nil

}
