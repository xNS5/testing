package middleware

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

var Logger zerolog.Logger

func Init() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339,
		FormatLevel: func(i any) string {
			return strings.ToUpper(fmt.Sprintf("[%s]", i))
		},
	}).Level(zerolog.TraceLevel).With().Timestamp().Logger()
}

func UnaryLoggingInterceptor(logger zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {

		start := time.Now()

		var remoteIP string
		if p, ok := peer.FromContext(ctx); ok {
			remoteIP = p.Addr.String()
		}

		var userAgent string
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if ua := md.Get("user-agent"); len(ua) > 0 {
				userAgent = ua[0]
			}
		}

		var evt *zerolog.Event

		resp, err = handler(ctx, req)

		if err != nil {
			evt = logger.Error()
		} else {
			evt = logger.Info()
		}

		evt.
			Str("grpc_method", info.FullMethod).
			Str("remote_ip", remoteIP).
			Str("user_agent", userAgent).
			Dur("duration_ms", time.Since(start))

		if err != nil {
			evt.Err(err).Msg("gRPC Request Failed")
		} else {
			evt.Msg("gRPC Request Completed")
		}

		return resp, err
	}
}
