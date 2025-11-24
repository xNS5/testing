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
	NumConns    int
	MaxPerConn  int
	Opts        []grpc.DialOption
	IdleTimeout time.Duration
	DialTimeout time.Duration
	ReqTimeout  time.Duration
}

func NewClient(pool *Pool) (*Conn, error) {
	conn, err := grpc.NewClient(pool.Target, pool.Cfg.Opts...)

	newConn := &Conn{
		ID:         uuid.New(),
		ClientConn: conn,
		timeout:    pool.Cfg.ReqTimeout,
	}

	newConn.state.Store(states.IDLE)

	return newConn, err
}

func NewPool(target string, cfg *PoolConfig) (*Pool, error) {
	if cfg.NumConns < 1 {
		return nil, fmt.Errorf("maxConns must be greater than zero")
	}

	pool := &Pool{
		Target: target,
		Cfg:    cfg,
		Conns:  make([]*Conn, cfg.NumConns),
	}

	for i := 0; i < cfg.NumConns; i++ {
		if conn, err := NewClient(pool); err == nil {
			pool.Conns[i] = conn
		}
	}

	return pool, nil
}

func (p *Pool) Get(ctx context.Context) (*Conn, error) {

	var best *Conn

	for i := range len(p.Conns) {
		if c := p.Conns[i]; c.canAccept(p.Cfg.MaxPerConn) {
			fmt.Println("Found best connection", c.ID)
			best = c
			break
		}
	}

	if best == nil {
		return nil, fmt.Errorf("")
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
