package grpc_pool

import (
	"context"

	"google.golang.org/grpc"
)

func (p *Pool) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

		conn, err := p.Get(ctx)
		if err != nil {
			return err
		}
		defer p.Release(conn)

		if _, ok := ctx.Deadline(); !ok && p.Timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, p.Timeout)
			defer cancel()
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
