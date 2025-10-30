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

		newHandler := WithHandlerCancellation(handler)

		return newHandler(ctx, req)
	}
}
