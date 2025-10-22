package tests

import (
	"context"
	"fmt"
	"grpc_client/grpc_pool"
	proto "grpc_client/protobuf"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConnection(t *testing.T) {

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
		os.Exit(-1)
	}

	ctx := context.Background()

	fmt.Println("Getting conn")

	conn, err := pool.Get(ctx)

	if err != nil {
		fmt.Println("Error initializing grpc pool")
		os.Exit(-1)
	}

	defer conn.Close()

	fmt.Println("Conn Success")

	client := proto.NewHelloClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	res, err := client.Hello(ctx, &proto.Request{})

	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	fmt.Println(res)

}
