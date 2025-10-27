package grpc_pool

import (
	"context"
	"fmt"
	"grpc_client/grpc_pool/states"

	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	return &Conn{
		ID:         uuid.New(),
		ClientConn: conn,
	}, err
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
		Conns:      []*Conn{conn},
		Target:     pool.Target,
		Opts:       pool.Opts,
		Timeout:    pool.Timeout,
		MaxConns:   pool.MaxConns,
		MaxPerConn: pool.MaxPerConn,
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

	if len(p.Conns) < p.MaxConns {

		if best == nil {
			conn, err := NewClient(p.Target, p.Opts)

			if err != nil {
				return nil, err
			}
			best = conn
			p.Conns = append(p.Conns, best)
		}

	} else {
		return nil, fmt.Errorf("pool is at capacity")
	}

	best.active.Add(1)

	return best, nil
}

func (p *Pool) Release(c *Conn) {
	p.Mtx.Lock()
	defer p.Mtx.Unlock()

	if c.active.Load() > 0 {
		c.active.Add(-1)
	}

	if c.active.Load() == 0 {
		c.state.Swap(states.IDLE)
	}
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
		if err := c.safeClose(); err != nil {
			fmt.Printf("Unable to safe close: %v", err)
		}
	}
}

/*
CONNECTION LIFECYCLE
*/
func (p *Pool) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	conn, err := p.Get(ctx)

	if err != nil {
		return fmt.Errorf("error invoking connection, %v", err)
	}

	defer p.Release(conn)

	return nil
}

func (p *Pool) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return nil, status.Error(codes.Unimplemented, "streaming not supported")
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
