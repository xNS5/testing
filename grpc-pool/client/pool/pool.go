package pool

import (
	"context"
	"fmt"
	"grpc_client/pool/states"

	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
)

type Pool struct {
	Mtx    sync.Mutex
	Conns  []*Conn
	Target string
	sem    *semaphore.Weighted
	Cfg    *PoolConfig
}

type PoolConfig struct {
	Conns         int
	MaxReqPerConn int
	Opts          []grpc.DialOption
	ReqTimeout    time.Duration
}

func NewClient(pool *Pool) (*Conn, error) {
	conn, err := grpc.NewClient(pool.Target, pool.Cfg.Opts...)

	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	newConn := &Conn{
		ID:         uuid.New(),
		ClientConn: conn,
		ref:        pool,
		sem:        semaphore.NewWeighted(int64(pool.Cfg.MaxReqPerConn)),
		timeout:    pool.Cfg.ReqTimeout,
	}

	newConn.state.Store(states.IDLE)

	return newConn, err
}

func NewPool(target string, cfg *PoolConfig) (*Pool, error) {
	if cfg.Conns < 1 {
		return nil, fmt.Errorf("maxConns must be greater than zero")
	}

	fmt.Println("Initializing pool")

	pool := &Pool{
		Target: target,
		Cfg:    cfg,
		sem:    semaphore.NewWeighted(int64(cfg.Conns)),
		Conns:  make([]*Conn, cfg.Conns),
	}

	for i := 0; i < cfg.Conns; i++ {
		if conn, err := NewClient(pool); err == nil {
			fmt.Println("Creating conn: ", conn.ID)
			pool.Conns[i] = conn
		} else {
			return nil, err
		}
	}

	return pool, nil
}

func (p *Pool) Get(ctx context.Context) (*Conn, error) {

	for _, c := range p.Conns {

		if c.active.Load() >= int32(p.Cfg.MaxReqPerConn) {
			continue
		}

		if c.sem.TryAcquire(1) {
			c.touch()
			fmt.Println(c.ID)
			return c, nil
		}
	}

	return nil, fmt.Errorf("pool at capacity")
}

func (p *Pool) Release() {
	p.sem.Release(1)
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
