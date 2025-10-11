package grpc_pool

import (
	"grpc_client/grpc_pool/states"
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
}

const idleThreshold = time.Duration(30 * time.Second)

func (c *Conn) touch() {
	if c.state.Load() >= states.CLOSING {
		return
	}
	c.lastUsed.Store(time.Now().UnixNano())
}

func (c *Conn) isIdle() bool {
	lastUsed := time.Unix(0, c.lastUsed.Load())
	elapsed := time.Since(lastUsed)

	currState := c.state.Load()

	return elapsed > idleThreshold && currState == states.IDLE && c.active.Load() == 0
}

func (c *Conn) canAccept(maxRPC int) bool {
	if c.state.Load() != states.IDLE && c.state.Load() != states.ALIVE {
		return false
	}

	return c.active.Load() < int32(maxRPC)
}

func (c *Conn) safeClose() {
	if !c.state.CompareAndSwap(states.IDLE, states.CLOSING) && !c.state.CompareAndSwap(states.ALIVE, states.CLOSING) {
		return
	}
	_ = c.ClientConn.Close()
	c.state.Store(int32(states.CLOSED))
}
