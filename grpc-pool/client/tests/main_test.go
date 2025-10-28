package tests

import (
	"context"
	proto "grpc_client/protobuf"
	"os"
	"testing"
)

func TestConnection(t *testing.T) {

	ctx := context.Background()

	pool, err := GetPool()

	if err != nil {
		t.Errorf("Error getting gRPC pool: %v", err)
		os.Exit(-1)
	}

	conn, err := pool.Get(ctx)

	if err != nil {
		t.Errorf("Error getting connection: %v", err)
		os.Exit(-1)
	}

	client := proto.NewHelloClient(conn)

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

	conn, err := pool.Get(ctx)

	if err != nil {
		t.Errorf("Error getting connection: %v", err)
		os.Exit(-1)
	}

	defer conn.Close()

	client := proto.NewHelloClient(conn)

	timeout := int32(10)

	res, err := client.Hello(ctx, &proto.Request{
		Timeout: &timeout,
	})

	if err != nil {
		t.Errorf("hello request error: %v", err)
		os.Exit(-1)
	}

	if res.Res != "Hello, world!" {
		t.Errorf("hello request error: %v", err)
		os.Exit(-1)
	}
}
