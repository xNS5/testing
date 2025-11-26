package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"grpc_client/pool"
	proto "grpc_client/protobuf"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

/*
TestConnection
Tests whether the pool can establish a connection
*/
func TestConnection(t *testing.T) {

	ctx := context.Background()

	service_config := map[string]any{
		"retryPolicy": map[string]any{
			"maxAttempts":          4,
			"initialBackoff":       "0.5s",
			"maxBackoff":           "4s",
			"backoffMultiplier":    2,
			"retryableStatusCodes": []string{"UNAVAILABLE"},
		},
	}

	data, err := json.Marshal(service_config)

	if err != nil {
		t.Errorf("error marshaling retry policy config")
		os.Exit(-1)
	}

	pool, Reset, err := GetPool(&pool.PoolConfig{
		Conns:         2,
		MaxReqPerConn: 2,
		Opts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(string(data)),
		},
		ReqTimeout: time.Duration(2 * time.Second),
	})

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

	// var msg *string

	_, err = client.Hello(ctx, &proto.Request{})

	if err != nil {
		t.Errorf("hello request error: %v", err)
		os.Exit(-1)
	}

	// assert.Equal(t, res.Res, fmt.Sprintf("Hello, world! %v", msg))
}

/*
TestTimeout
Tests whether the RPC connection times out within the duration set in the pool
Note: Server should also time out, interrupting any blocking requests (in theory)
*/
func TestTimeout(t *testing.T) {

	ctx := context.Background()

	pool, Reset, err := GetPool(&pool.PoolConfig{
		MaxReqPerConn: 2,
		Opts:          []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		ReqTimeout:    time.Duration(2 * time.Second),
	})

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
TestConcurrentGet
Tests running n connection requests concurrently to the server.
Expected result: the pool creates ( numConns // numPerCon ) connections
*/
func TestConcurrentGet(t *testing.T) {

	ctx := context.Background()

	pool, Reset, err := GetPool(&pool.PoolConfig{
		MaxReqPerConn: 2,
		Opts:          []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		ReqTimeout:    time.Duration(2 * time.Second),
	})

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
}

/*
TestConcurrentGetOverflow
Tests running n connection requests concurrently to the server.
Expected result: the pool creates ( numConns // numPerCon ) connections
*/
func TestConcurrentGetOverflow(t *testing.T) {

	ctx := context.Background()

	pool, Reset, err := GetPool(&pool.PoolConfig{
		MaxReqPerConn: 2,
		Opts:          []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		ReqTimeout:    time.Duration(10 * time.Second),
	})

	defer Reset()

	if err != nil {
		t.Errorf("Error getting gRPC pool: %v", err)
		os.Exit(-1)
	}

	var wg sync.WaitGroup

	reqs := 5
	// In theory, 5 connections and 2 rejected request (because math)

	wg.Add(reqs)

	var NumErrors atomic.Int32

	timeout := int32(5)

	for i := range reqs {
		go func() {
			defer wg.Done()

			conn, err := pool.Get(ctx)

			if err != nil {
				fmt.Printf("Error getting connection: %v\r\n", err)
				NumErrors.Add(1)
				return
			}

			client := proto.NewHelloClient(conn)

			msg := fmt.Sprintf("Test New Conn %v", i)

			res, err := client.Hello(ctx, &proto.Request{
				Timeout: &timeout,
				Msg:     &msg,
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

	assert.Equal(t, 2, NumErrors.Load())
}
