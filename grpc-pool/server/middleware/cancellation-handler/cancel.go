package cancellationhandler

import (
	"context"
	"fmt"
)

func WithHandlerCancellation(
	handler func(ctx context.Context, req any) (any, error),
) func(ctx context.Context, req any) (any, error) {

	return func(ctx context.Context, req any) (any, error) {
		respCh := make(chan struct {
			resp any
			err  error
		}, 1)

		go func() {
			r, err := handler(ctx, req)
			respCh <- struct {
				resp any
				err  error
			}{r, err}
		}()

		select {
		case r := <-respCh:
			return r.resp, r.err
		case <-ctx.Done():
			return nil, fmt.Errorf("handler canceled")
		}
	}
}
