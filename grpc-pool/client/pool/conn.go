package pool

import (
	"context"
	"grpc_client/pool/states"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type Conn struct {
	*grpc.ClientConn
	ID        uuid.UUID
	timeout   time.Duration
	last_used atomic.Int64
	active    atomic.Int32
	state     atomic.Int32
}

// const idleThreshold = time.Duration(30 * time.Second)

/*
CONNECTION LIFECYCLE
*/
func (c *Conn) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {

	c.state.Store(states.ALIVE)
	ctx, cancel := context.WithTimeout(ctx, c.timeout)

	c.touch()
	defer c.release()

	go func() {
		<-ctx.Done()
		cancel()
	}()

	return c.ClientConn.Invoke(ctx, method, args, reply, opts...)
}

func (c *Conn) release() {
	if c.active.Load() > 0 {
		c.active.Add(-1)
	} else {
		c.state.CompareAndSwap(states.ALIVE, states.IDLE)
	}
}

func (c *Conn) canAccept(maxPerRpc int) bool {
	return c.active.Load() < int32(maxPerRpc)
}

func (c *Conn) touch() {
	c.active.Add(1)
	c.last_used.Store(time.Now().Unix())
}
