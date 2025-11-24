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
	ID      uuid.UUID
	timeout time.Duration
	active  atomic.Int32
	state   atomic.Int32
}

// const idleThreshold = time.Duration(30 * time.Second)

/*
CONNECTION LIFECYCLE
*/
func (c *Conn) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {

	c.state.Store(states.ALIVE)
	ctx, cancel := context.WithTimeout(ctx, c.timeout)

	c.active.Add(1)
	defer c.release()

	go func() {
		<-ctx.Done()
		cancel()
	}()

	return c.ClientConn.Invoke(ctx, method, args, reply, opts...)
}

func (c *Conn) release() error {
	if c.active.Load() > 0 {
		c.active.Add(-1)
	}
	return nil
}

func (c *Conn) canAccept(maxPerRpc int) bool {
	return c.active.Load() < int32(maxPerRpc)
}

func (c *Conn) touch() {

}
