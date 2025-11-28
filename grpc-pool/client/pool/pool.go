package pool

import (
	"context"
	"fmt"
	"grpc_client/pool/states"

	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type Pool struct {
	Mtx    sync.Mutex
	Conns  []*Conn
	Target string
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
		timeout:    pool.Cfg.ReqTimeout,
	}

	newConn.state.Store(states.IDLE)

	return newConn, err
}

func NewPool(target string, cfg *PoolConfig) (*Pool, error) {
	if cfg.Conns < 1 {
		return nil, fmt.Errorf("maxConns must be greater than zero")
	}

	pool := &Pool{
		Target: target,
		Cfg:    cfg,
		Conns:  make([]*Conn, cfg.Conns),
	}

	for i := 0; i < cfg.Conns; i++ {
		if conn, err := NewClient(pool); err == nil {
			pool.Conns[i] = conn
		} else {
			return nil, err
		}
	}

	return pool, nil
}

func (p *Pool) Get(ctx context.Context) (*Conn, error) {

	var best *Conn

	for i := range len(p.Conns) {
		if c := p.Conns[i]; c.canAccept(p.Cfg.MaxReqPerConn) {
			fmt.Println("Found best connection", c.ID)
			best = c
			break
		}
	}

	if best == nil {
		return nil, fmt.Errorf("pool at capacity")
	}

	return best, nil
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
