package tests

import (
	"context"
	"fmt"
	proto "grpc_client/protobuf"
	"os"
	"sync"
	"testing"
)

func Reset() {
	Pool = nil
}

/*
TestConnection
Tests whether the pool can establish a connection
*/
func TestConnection(t *testing.T) {

	ctx := context.Background()

	pool, err := GetPool()

	defer Reset()

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

	var msg *string

	res, err := client.Hello(ctx, &proto.Request{})

	if err != nil {
		t.Errorf("hello request error: %v", err)
		os.Exit(-1)
	}

	if res.Res != fmt.Sprintf("Hello, world! %v", msg) {
		t.Errorf("hello request error: %v", res.Res)
		os.Exit(-1)
	}

}

/*
TestTimeout
Tests whether the RPC connection times out within the duration set in the pool
Note: Server should also time out, interrupting any blocking requests (in theory)
*/
func TestTimeout(t *testing.T) {

	ctx := context.Background()

	pool, err := GetPool()

	defer Reset()

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

	_, err = client.Hello(ctx, &proto.Request{
		Timeout: &timeout,
	})

	if err == nil {
		t.Errorf("hello request error: %v", err)
		os.Exit(-1)
	}
}

/*
TestNewConn
Tests filling up a conn with multiplexed requests, should create new server connection in the pool
*/
func TestNewConn(t *testing.T) {

	ctx := context.Background()

	pool, err := GetPool()

	defer Reset()

	if err != nil {
		t.Errorf("Error getting gRPC pool: %v", err)
		os.Exit(-1)
	}

	var wg sync.WaitGroup

	conns := 4

	wg.Add(conns)

	for i := range conns {
		go func() {

			defer wg.Done()
			conn, err := pool.Get(ctx)

			if err != nil {
				t.Errorf("Error getting connection: %v", err)
				os.Exit(-1)
			}

			defer conn.Close()

			client := proto.NewHelloClient(conn)

			// timeout := int32(1)

			msg := fmt.Sprintf("Test New Conn %v", i)

			res, err := client.Hello(ctx, &proto.Request{
				Msg: &msg,
			})

			if err != nil {
				t.Errorf("hello request error: %v", err)
				os.Exit(-1)
			}

			if res.Res != msg {
				t.Errorf("hello request error: %v", err)
				os.Exit(-1)
			}

		}()
	}
	wg.Wait()
}
