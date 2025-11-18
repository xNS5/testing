package pool

import (
	"context"
	"fmt"
	"grpc_client/pool/states"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type Conn struct {
	*grpc.ClientConn
	ID       uuid.UUID
	active   atomic.Int32
	state    atomic.Int32
	lastUsed atomic.Int64
	Timeout  time.Duration
}

// const idleThreshold = time.Duration(30 * time.Second)

/*
CONNECTION LIFECYCLE
*/
func (c *Conn) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {

	ctx, cancel := context.WithTimeout(ctx, c.Timeout)

	go func() {
		<-ctx.Done()
		cancel()
	}()

	return c.ClientConn.Invoke(ctx, method, args, reply, opts...)
}

func (c *Conn) touch() error {

	if c.state.Load() >= states.CLOSING {
		return fmt.Errorf("conn is closing")
	}

	c.active.Add(1)
	c.lastUsed.Store(time.Now().UnixNano())
	c.state.Swap(states.ALIVE)

	return nil
}

// func (c *Conn) isIdle() bool {
// 	lastUsed := time.Unix(0, c.lastUsed.Load())
// 	elapsed := time.Since(lastUsed)

// 	return elapsed > idleThreshold && c.active.Load() > -1
// }

func (c *Conn) canAccept(maxPerRpc int) bool {
	if c.state.Load() >= states.CLOSING {
		return false
	}

	return c.active.Load() < int32(maxPerRpc)
}

// func (c *Conn) safeClose() error {

// 	fmt.Println("trying co safe close...")
// 	if !c.isIdle() && !c.state.CompareAndSwap(states.CLOSING, states.CLOSED) /* && !c.state.CompareAndSwap(states.ALIVE, states.CLOSING) */ {
// 		return fmt.Errorf("unable to change conn state to closing: %v", c.state.Load())
// 	}

// 	_ = c.ClientConn.Close()
// 	c.state.Store(states.CLOSED)
// 	return nil
// }
