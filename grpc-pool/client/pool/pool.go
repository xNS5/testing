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
	Mtx        sync.Mutex
	Conns      []*Conn
	Target     string
	Opts       []grpc.DialOption
	Timeout    time.Duration
	RPCTimeout time.Duration
	MaxConns   int
	MaxPerConn int
}

func NewClient(pool *Pool) (*Conn, error) {
	conn, err := grpc.NewClient(pool.Target, pool.Opts...)

	newConn := &Conn{
		ID:         uuid.New(),
		ClientConn: conn,
		timeout:    pool.RPCTimeout,
		PoolRef:    pool,
	}

	newConn.state.Store(states.IDLE)

	return newConn, err
}

func NewPool(pool *Pool) (*Pool, error) {
	if pool.MaxConns <= 0 {
		return nil, fmt.Errorf("maxConns must be greater than zero")
	}

	conn, err := NewClient(pool)

	if err != nil {
		return nil, err
	}

	pool.Conns = []*Conn{conn}

	return pool, nil
}

func (p *Pool) Get(ctx context.Context) (*Conn, error) {

	p.Mtx.Lock()
	defer p.Mtx.Unlock()

	var best *Conn

	for _, c := range p.Conns {
		if c.canAccept(p.MaxPerConn) {
			fmt.Println("Found best connection", c.ID)
			best = c
			break
		}
	}

	if len(p.Conns) <= p.MaxConns {
		if best == nil {
			conn, err := NewClient(p)
			if err != nil {
				return nil, err
			}

			best = conn

			fmt.Println("Connection full, creating new client", conn.ID)

			p.Conns = append(p.Conns, best)
		}
	} else {
		return nil, fmt.Errorf("pool is at capacity")
	}

	fmt.Println(len(p.Conns))

	best.touch()

	// fmt.Printf("Num Conns: %v\r\nBest ID: %v\n", len(p.Conns), best.ID)

	return best, nil
}

func (p *Pool) Release(c *Conn) {
	curr_load := c.active.Load()
	if curr_load > 0 {
		c.active.Add(-1)
	} else if curr_load == 0 {
		c.state.CompareAndSwap(states.ALIVE, states.IDLE)
	}
}

func (p *Pool) Clean() {
	if !p.Mtx.TryLock() {
		return
	}
	defer p.Mtx.Unlock()

	if len(p.Conns) == 0 {
		fmt.Println("No connections, skipping...")
		return
	} else {
		fmt.Println("Beginning cleanup")
	}

	alive_conns := make([]*Conn, 0, len(p.Conns))
	to_close := make(chan *Conn)

	for i, c := range p.Conns {
		if i == 0 || !c.isIdle() {
			alive_conns = append(alive_conns, c)
			fmt.Printf("Keeping: %v\n", c.ID)
		}

		if c.state.CompareAndSwap(states.IDLE, states.CLOSING) {
			fmt.Println("Closing ", c.ID)
			to_close <- c
		}
	}

	close(to_close)

	p.Conns = alive_conns

	for c := range to_close {
		fmt.Println(c.ID)
		if err := c.safeClose(); err != nil {
			fmt.Printf("Unable to safe close: %v\n", err)
		} else {
			fmt.Printf("Closing: %v\n", c.ID)
		}
	}
}

func (p *Pool) ScheduledCleanup(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)

	go func() {
		for {
			<-ticker.C
			fmt.Println("Ticked, cleaning...")
			p.Clean()
		}
	}()

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
