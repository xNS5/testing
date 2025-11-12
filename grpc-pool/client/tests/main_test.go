package tests

import (
	"context"
	"fmt"
	proto "grpc_client/protobuf"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
TestConnection
Tests whether the pool can establish a connection
*/
func TestConnection(t *testing.T) {

	ctx := context.Background()

	pool, Reset, err := GetPool()

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

	assert.Equal(t, res.Res, fmt.Sprintf("Hello, world! %v", msg))
}

/*
TestTimeout
Tests whether the RPC connection times out within the duration set in the pool
Note: Server should also time out, interrupting any blocking requests (in theory)
*/
func TestTimeout(t *testing.T) {

	ctx := context.Background()

	pool, Reset, err := GetPool()

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

	timeout := int32(10)

	_, err = client.Hello(ctx, &proto.Request{
		Timeout: &timeout,
	})

	assert.NotNil(t, err)
}

/*
TestNewConn
Tests filling up a conn with multiplexed requests, should create new server connection in the pool
*/
func TestNewConn(t *testing.T) {

	ctx := context.Background()

	pool, Reset, err := GetPool()

	defer Reset()

	if err != nil {
		t.Errorf("Error getting gRPC pool: %v", err)
		os.Exit(-1)
	}

	var wg sync.WaitGroup

	reqs := 4
	// In theory, 2 connections total

	wg.Add(reqs)

	for i := range reqs {
		go func() {
			defer wg.Done()
			conn, err := pool.Get(ctx)

			if err != nil {
				t.Errorf("Error getting connection: %v", err)
				os.Exit(-1)
			}

			client := proto.NewHelloClient(conn)

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

	assert.Equal(t, 2, len(pool.Conns))
}

func TestConcurrentGet(t *testing.T) {
	ctx := context.Background()

	pool, Reset, err := GetPool()

	defer Reset()

	if err != nil {
		t.Errorf("Error getting gRPC pool: %v", err)
		os.Exit(-1)
	}

	var wg sync.WaitGroup

	for range 4 {
		conn, err := pool.Get(ctx)

		if err != nil {
			t.Errorf("Error getting connection: %v", err)
			os.Exit(-1)
		}

		client := proto.NewHelloClient(conn)

		wg.Go(func() {

			// msg := fmt.Sprintf("Test New Conn %v", i)
			timeout := int32(5)

			_, err = client.Hello(ctx, &proto.Request{
				// Msg:     &msg,
				Timeout: &timeout,
			})

			if err != nil {
				t.Errorf("hello request error: %v", err)
				os.Exit(-1)
			}

			// if res.Res != msg {
			// 	t.Errorf("hello request error: %v", err)
			// 	os.Exit(-1)
			// }
		})
	}

	wg.Wait()
	assert.Equal(t, 2, len(pool.Conns))
}
