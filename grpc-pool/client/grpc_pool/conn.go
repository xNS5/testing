package grpc_pool

import (
	"context"
	"fmt"
	"grpc_client/grpc_pool/states"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type Conn struct {
	*grpc.ClientConn
	PoolRef  *Pool
	ID       uuid.UUID
	timeout  time.Duration
	active   atomic.Int32
	state    atomic.Int32
	lastUsed atomic.Int64
}

const idleThreshold = time.Duration(30 * time.Second)

/*
CONNECTION LIFECYCLE
*/
func (c *Conn) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	return c.PoolRef.Invoke(ctx, method, args, reply, opts...)
}

func (c *Conn) touch() {
	if c.state.Load() >= states.CLOSING {
		return
	}

	c.state.Swap(states.ALIVE)
	c.active.Add(1)
	c.lastUsed.Store(time.Now().UnixNano())
}

func (c *Conn) isIdle() bool {
	lastUsed := time.Unix(0, c.lastUsed.Load())
	elapsed := time.Since(lastUsed)

	return elapsed > idleThreshold && c.active.Load() == 0
}

func (c *Conn) canAccept(maxRPC int) bool {
	if c.state.Load() != states.IDLE && c.state.Load() != states.ALIVE {
		return false
	}

	return c.active.Load() < int32(maxRPC)+1
}

func (c *Conn) safeClose() error {
	fmt.Println("trying co safe close...")
	if !c.isIdle() && !c.state.CompareAndSwap(states.CLOSING, states.CLOSED) /* && !c.state.CompareAndSwap(states.ALIVE, states.CLOSING) */ {
		return fmt.Errorf("unable to change conn state to closing: %v", c.state.Load())
	}

	_ = c.ClientConn.Close()
	c.state.Store(states.CLOSED)
	return nil
}
