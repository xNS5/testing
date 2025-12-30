package pool

import (
	"context"
	"fmt"
	"grpc_client/pool/states"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
)

type Conn struct {
	*grpc.ClientConn
	ID        uuid.UUID
	timeout   time.Duration
	last_used atomic.Int64
	active    atomic.Int32
	sem       *semaphore.Weighted
	ref       *Pool
	state     atomic.Int32
}

// const idleThreshold = time.Duration(30 * time.Second)

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

/*
CONNECTION LIFECYCLE
*/
func (c *Conn) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {

	c.state.Store(states.ALIVE)
	ctx, cancel := context.WithTimeout(ctx, c.timeout)

	go func() {
		<-ctx.Done()
		cancel()
		c.Release()
	}()

	return c.ClientConn.Invoke(ctx, method, args, reply, opts...)
}

func (c *Conn) TryAcquire() bool {
	return c.sem.TryAcquire(1)
}

func (c *Conn) Release() {
	if c.active.Load() > 0 {
		c.active.Add(-1)
		c.sem.Release(1)
	} else {
		c.state.Store(states.IDLE)
	}
}

func (c *Conn) touch() {
	c.active.Add(1)
	c.state.Store(states.ALIVE)
	c.last_used.Store(time.Now().Unix())
}
