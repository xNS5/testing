package grpc_pool

import (
	"context"
	"fmt"
	"grpc_client/grpc_pool/states"

	"sync"
	"time"

	"google.golang.org/grpc"
)

type Pool struct {
	Mtx        sync.Mutex
	Conns      []*Conn
	Target     string
	Opts       []grpc.DialOption
	Timeout    time.Duration
	MaxConns   int
	MaxPerConn int
}

func NewClient(target string, opts []grpc.DialOption) (*Conn, error) {
	conn, err := grpc.NewClient(target, opts...)

	return &Conn{ClientConn: conn}, err
}

func NewPool(pool *Pool) (*Pool, error) {
	if pool.MaxConns <= 0 {
		return nil, fmt.Errorf("MaxConns must be greater than zero")
	}

	conn, err := NewClient(pool.Target, pool.Opts)

	if err != nil {
		return nil, err
	}

	return &Pool{
		Conns:    []*Conn{conn},
		Target:   pool.Target,
		Opts:     pool.Opts,
		MaxConns: pool.MaxConns,
	}, nil
}

func (p *Pool) Get(ctx context.Context) (*Conn, error) {
	fmt.Println("Locking Mutex")
	p.Mtx.Lock()

	defer p.Mtx.Unlock()

	var best *Conn

	for _, c := range p.Conns {
		if c.canAccept(p.MaxPerConn) {
			fmt.Println("Found best connection", c.ID)
			best = c
			c.touch()
			break
		}
	}

	if best == nil && len(p.Conns) < p.MaxConns {
		conn, err := NewClient(p.Target, p.Opts)

		if err != nil {
			return nil, err
		}
		best = conn
		p.Conns = append(p.Conns, best)
	} else {
		return nil, fmt.Errorf("pool is at capacity")
	}
	best.active.Add(1)

	return best, nil
}

func (p *Pool) Release(c *Conn) {
	p.Mtx.Lock()
	defer p.Mtx.Unlock()
	c.active.Add(-1)
}

func (p *Pool) DoUnary(ctx context.Context, f func(conn *grpc.ClientConn) error) error {

	conn, err := p.Get(ctx)
	if err != nil {
		return err
	}
	defer p.Release(conn)

	return f(conn.ClientConn)
}

func (p *Pool) Clean() {
	p.Mtx.Lock()

	alive_conns := make([]*Conn, 0, len(p.Conns))
	to_close := make([]*Conn, 0)

	for i, c := range p.Conns {
		if i == 0 || !c.isIdle() {
			alive_conns = append(alive_conns, c)
		}

		if c.state.CompareAndSwap(states.IDLE, states.CLOSING) {
			to_close = append(to_close, c)
		}
	}

	p.Conns = alive_conns
	p.Mtx.Unlock()

	for _, c := range to_close {
		c.safeClose()
	}
}
