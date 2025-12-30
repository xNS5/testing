package pool

import (
	"context"
	"fmt"
	proto "grpc_client/protobuf"
	"math"
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

	pool, err := getPool(&PoolConfig{
		Conns:         2,
		MaxReqPerConn: 2,
		Opts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		ReqTimeout: time.Duration(20 * time.Second),
	}, nil)

	if err != nil {
		t.Fatalf("Error getting gRPC pool: %v", err)
	}

	conn, err := pool.Get(ctx)

	if err != nil {
		t.Fatalf("Error getting connection: %v", err)
	}

	defer conn.Release()


	client := proto.NewHelloClient(conn)


	_, err = client.Hello(ctx, &proto.Request{})

	assert.Nil(t, err)
}

/*
TestErrorConnection
Tests that a valid error response from the server (in this case no connection) is handled properly and an error is returned
*/

func TestErrorConnection(t *testing.T) {

	ctx := context.Background()

	target := "localhost:5051"

	pool, err := getPool(&PoolConfig{
		Conns:         2,
		MaxReqPerConn: 2,
		Opts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		ReqTimeout: time.Duration(20 * time.Second),
	}, &target)

	if err != nil {
		t.Fatalf("Error getting gRPC pool: %v", err)
	}

	conn, err := pool.Get(ctx)

	if err != nil {
		t.Fatalf("Error getting connection: %v", err)
	}

	defer conn.Release()


	client := proto.NewHelloClient(conn)

	_, err = client.Hello(ctx, &proto.Request{})

	assert.NotNil(t, err)
}

/*
TestTimeout
Tests whether the RPC connection times out within the duration set in the pool
Note: Server should also time out, interrupting any blocking requests (in theory)
*/
func TestTimeout(t *testing.T) {

	ctx := context.Background()

	pool, err := getPool(&PoolConfig{
		Conns:         2,
		MaxReqPerConn: 2,
		Opts:          []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		ReqTimeout:    time.Duration(2 * time.Second),
	}, nil)

	if err != nil {
		t.Fatalf("Error getting gRPC pool: %v", err)
	}

	conn, err := pool.Get(ctx)

	if err != nil {
		t.Fatalf("Error getting connection: %v", err)
	}

	defer conn.Release()


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

	pool, err := getPool(&PoolConfig{
		Conns:         2,
		MaxReqPerConn: 2,
		Opts:          []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		ReqTimeout:    time.Duration(2 * time.Second),
	}, nil)

	if err != nil {
		t.Fatalf("Error getting gRPC pool: %v", err)
	}

	var wg sync.WaitGroup

	reqs := 4

	errs := make(chan error, reqs)

	wg.Add(reqs)

	for i := range reqs {
		go func() {
			defer wg.Done()
			conn, err := pool.Get(ctx)
		
			if err != nil {
				t.Errorf("Error getting connection: %v", err)
				errs <- err
				return
			}

			defer conn.Release()

			client := proto.NewHelloClient(conn)

			msg := fmt.Sprintf("Test New Conn %v", i)

			res, err := client.Hello(ctx, &proto.Request{
				Msg: &msg,
			})

			if err != nil {
				t.Errorf("hello request error: %v", err)
				errs <- err
				return
			}

			if res.Res != msg {
				t.Errorf("hello request error: %v", err)
				errs <- err
				return
			}

		}()
	}
	wg.Wait()
	close(errs)

	if len(errs) > 0 {
		err := <- errs;
		if err != nil {
			t.Errorf("Error detected: %v", err)
		}
	}	

	assert.Equal(t, len(errs), 0)
}

/*
TestConcurrentGetOverflow
Tests running n connection requests concurrently to the server.
Expected result: the pool creates ( numConns // numPerCon ) connections
*/
func TestConcurrentGetOverflow(t *testing.T) {

	ctx := context.Background()

	pool, err := getPool(&PoolConfig{
		Conns:         3,
		MaxReqPerConn: 2,
		Opts:          []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		ReqTimeout:    time.Duration(10 * time.Second),
	}, nil)

	if err != nil {
		t.Errorf("Error getting gRPC pool: %v", err)
		os.Exit(-1)
	}

	var wg sync.WaitGroup

	reqs := 8
	wg.Add(reqs)

	var numErrors atomic.Int32

	timeout := int32(5)

	for range reqs {
		go func() {
			defer wg.Done()

			conn, err := pool.Get(ctx)

			if err != nil {
				fmt.Println("Err: ", err)
				numErrors.Add(1)
				return
			}

			client := proto.NewHelloClient(conn)

			_, _ = client.Hello(ctx, &proto.Request{
				Timeout: &timeout,
			})
		}()
	}

	wg.Wait()

	assert.Equal(t, int(math.Abs(float64((pool.Cfg.MaxReqPerConn*pool.Cfg.Conns)-reqs))), int(numErrors.Load()))
}

// TestConfig

func TestMethodConfig(t *testing.T) {

	ctx := context.Background()

	service_cfg := ServiceConfig{
		MethodConfig: []MethodConfig{
			{
				Name: []Name{{}},
				RetryPolicy: &RetryPolicy{
					MaxAttempts:          4,
					InitialBackoff:       "2s",
					MaxBackoff:           "4s",
					BackoffMultiplier:    2,
					RetryableStatusCodes: []string{"UNAVAILABLE"},
				},
			},
		},
	}

	data, err := service_cfg.ToJSON()

	if err != nil {
		t.Errorf("Unable to parse config: %v", err)
		os.Exit(-1)
	}

	target := "localhost:2020"

	pool, err := NewPool(target, (&PoolConfig{
		Conns:         2,
		MaxReqPerConn: 2,
		Opts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(string(data)),
		},
		ReqTimeout: time.Duration(20 * time.Second),
	}))

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

	retry_policy := service_cfg.MethodConfig[0].RetryPolicy

	max_attempts := retry_policy.MaxAttempts
	multiplier := float64(retry_policy.BackoffMultiplier)
	initial_backoff := time.Duration(int(retry_policy.InitialBackoff[0]) * time.Now().Second())
	max_backoff := time.Duration(int(retry_policy.MaxBackoff[0]) * time.Now().Second())

	estimated_duration := totalRetryBackoff(max_attempts, time.Duration(initial_backoff), max_backoff, multiplier)

	start := time.Now()

	// Not checking the resulting error as this is just testing that the retry configruation has worked
	_, _ = client.Hello(ctx, &proto.Request{})

	elapsed := time.Since(start)

	// Loose guesstimate, need to take into account jitte
	assert.GreaterOrEqual(t, elapsed, estimated_duration)
}
