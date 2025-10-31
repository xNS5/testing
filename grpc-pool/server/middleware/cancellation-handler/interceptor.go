package cancellationhandler

import (
	"context"

	"google.golang.org/grpc"
)

func UnaryCancellationInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {

		cancelWrapper := WithHandlerCancellation(handler)

		return cancelWrapper(ctx, req)
	}
}
