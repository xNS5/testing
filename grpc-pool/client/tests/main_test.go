package tests

import (
	"context"
	"fmt"
	proto "grpc_client/protobuf"
	"os"
	"testing"
	"time"
)

var MODE = 1 // 1 debug 0...not debug?

func TestConnection(t *testing.T) {

	ctx := context.Background()

	pool, err := GetPool()

	if err != nil {
		t.Errorf("Error getting gRPC pool: %v", err)
		os.Exit(-1)
	}

	if MODE == 1 {
		fmt.Println("Getting conn")
	}

	conn, err := pool.Get(ctx)

	if err != nil {
		t.Errorf("Error getting connection: %v", err)
		os.Exit(-1)
	}

	defer conn.Close()

	if MODE == 1 {
		fmt.Println("Conn Success")
	}

	client := proto.NewHelloClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	defer pool.Release(conn)

	res, err := client.Hello(ctx, &proto.Request{})

	if err != nil {
		t.Errorf("hello request error: %v", err)
		os.Exit(-1)
	}

	if res.Res != "Hello, world!" {
		t.Errorf("hello request error: %v", err)
		os.Exit(-1)
	}
}

func TestTimeout(t *testing.T) {

	ctx := context.Background()

	pool, err := GetPool()

	if err != nil {
		t.Errorf("Error getting gRPC pool: %v", err)
		os.Exit(-1)
	}

	if MODE == 1 {
		fmt.Println("Getting conn")
	}

	conn, err := pool.Get(ctx)

	if err != nil {
		t.Errorf("Error getting connection: %v", err)
		os.Exit(-1)
	}

	defer conn.Close()

	if MODE == 1 {
		fmt.Println("Conn Success")
	}

	client := proto.NewHelloClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()
	defer pool.Release(conn)

	res, err := client.Hello(ctx, &proto.Request{})

	if err != nil {
		t.Errorf("hello request error: %v", err)
		os.Exit(-1)
	}

	if res.Res != "Hello, world!" {
		t.Errorf("hello request error: %v", err)
		os.Exit(-1)
	}
}
