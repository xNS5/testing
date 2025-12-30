package pool

import (
	"context"
	"fmt"
	"sync"

	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type Pool struct {
	Conns  []*Conn
	Target string
	// sem    *semaphore.Weighted
	logger Logger
	logLevel LogLevel
	Cfg *PoolConfig
}

type PoolConfig struct {
	Conns         int
	MaxReqPerConn int
	Opts          []grpc.DialOption
	ReqTimeout    time.Duration
}

func NewPool(target string, cfg *PoolConfig, opts ...PoolOption) (*Pool, error) {
	if cfg.Conns < 1 {
		return nil, fmt.Errorf("maxConns must be greater than zero")
	}

	pool := &Pool{
		Target: target,
		Cfg:    cfg,
		/* // This is for later if I decide to make the pool scale dynamically
		sem:    semaphore.NewWeighted(int64(cfg.Conns)), */
		logger: NopLogger{},
		logLevel: Debug,
		Conns: make([]*Conn, cfg.Conns),
	}

	
	
	for _, opt := range opts {
		opt(pool)
	}
	
	if pool.logLevel == Debug {
		pool.logger.Debug("Initializing pool")
	}
	
	errs := make(chan error, cfg.Conns)
	conn_ids := make(chan uuid.UUID, cfg.Conns)

	var wg sync.WaitGroup
	wg.Add(cfg.Conns)

	for i := 0; i < cfg.Conns; i++ {
		go func(i int) {
			defer wg.Done()
			 if conn, err := NewClient(pool); err == nil {
				conn_ids <- conn.ID
				pool.Conns[i] = conn
			} else {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	close(conn_ids)

	if err := <-errs; err != nil {
		pool.logger.Error("Error detected, tearing down server: ", err)
		pool.GracefulShutdown()
		return nil, err
	}

	if pool.logLevel == Info {
		for id := range conn_ids {
			pool.logger.Info("Creating connection: ", id)
		}
	}

	return pool, nil
}

func (p *Pool) Get(ctx context.Context) (*Conn, error) {

	for _, c := range p.Conns {

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
