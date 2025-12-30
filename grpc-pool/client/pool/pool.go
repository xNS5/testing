package pool

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"time"

	"google.golang.org/grpc"
)

type Pool struct {
	Conns  []*Conn
	Target string
	// sem    *semaphore.Weighted
	Cfg *PoolConfig
}

type PoolConfig struct {
	Conns         int
	MaxReqPerConn int
	Opts          []grpc.DialOption
	ReqTimeout    time.Duration
}

func NewPool(target string, cfg *PoolConfig) (*Pool, error) {
	if cfg.Conns < 1 {
		return nil, fmt.Errorf("maxConns must be greater than zero")
	}

	fmt.Println("Initializing pool")

	pool := &Pool{
		Target: target,
		Cfg:    cfg,
		/* // This is for later if I decide to make the pool scale dynamically
		sem:    semaphore.NewWeighted(int64(cfg.Conns)), */
		Conns: make([]*Conn, cfg.Conns),
	}

	errs := make(chan error, cfg.Conns)
	var wg sync.WaitGroup
	wg.Add(cfg.Conns)

	for i := 0; i < cfg.Conns; i++ {
		
		go func(i int) {
			defer wg.Done()
			if math.Mod(float64(i)+1, 2) == 0 {
				errs <- errors.New("testing forced error return")
			} else if conn, err := NewClient(pool); err == nil {
				fmt.Println("Creating conn: ", i, conn.ID)
				pool.Conns[i] = conn
			} else {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	if err := <-errs; err != nil {
		fmt.Println("Error detected, tearing down server: ", err)
		pool.GracefulShutdown()
		return nil, err
	}

	return pool, nil
}

func (p *Pool) Get(ctx context.Context) (*Conn, error) {

	for _, c := range p.Conns {

		if c.active.Load() >= int32(p.Cfg.MaxReqPerConn) {
			continue
		}

		if c.TryAcquire() {
			c.touch()
			return c, nil
		}
	}

	return nil, fmt.Errorf("pool at capacity")
}

// func (p *Pool) Release() {
// 	p.sem.Release(1)
// }

func (p *Pool) GracefulShutdown() {
	for _, conn := range p.Conns {
		if conn != nil {
			conn.Close()
		}
	}
}

/*
INTERCEPTORS
*/
func (p *Pool) LoggingInterceptor(ctx context.Context,
	method string,
	req any,
	reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	return invoker(ctx, method, req, reply, cc, opts...)
}
