package pool

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

func totalRetryBackoff(maxAttempts int, initialBackoff, maxBackoff time.Duration, multiplier float64) time.Duration {
	var total time.Duration

	for i := 0; i < maxAttempts-1; i++ {
		backoff := min(time.Duration(float64(initialBackoff)*pow(multiplier, float64(i))), maxBackoff)
		total += backoff
	}

	return total
}

func pow(x, y float64) float64 {
	result := 1.0
	for i := 0; i < int(y); i++ {
		result *= x
	}
	return result
}

func getPool(config *PoolConfig, customTarget *string, opts ...PoolOption) (*Pool, error) {

	target := "localhost:5050"

	if customTarget != nil {
		target = *customTarget
	}

	fmt.Println("Initializing gRPC Pool")

	pool, err := NewPool(target, config)

	if err != nil {
		fmt.Println("Error initializing grpc pool")
	}

	return pool, err
}

type ZeroLogger struct {
	log zerolog.Logger
}

func addFields(e *zerolog.Event, kv ...any) {
	if math.Mod(float64(len(kv)), 2) != 0 {
		strs := make([]string, len(kv))
		for i, v := range kv {
			strs[i] = fmt.Sprint(v)
		}
		e.Interface("msg", strings.Join(strs, " "))
	} else {
		for i := 0; i+1 < len(kv); i += 2 {
			key, ok := kv[i].(string)

			if !ok {
				continue
			}
			e.Interface(key, kv[i+1])
		}
	}

}

func NewZeroLogger(log zerolog.Logger) ZeroLogger {
	return ZeroLogger{log: log}
}

func (z ZeroLogger) Debug(msg string, kv ...any) {
	e := z.log.Debug()
	addFields(e, kv...)
	e.Msg(msg)
}

func (z ZeroLogger) Info(msg string, kv ...any) {
	e := z.log.Info()
	addFields(e, kv...)
	e.Msg(msg)
}

func (z ZeroLogger) Warn(msg string, kv ...any) {
	e := z.log.Warn()
	addFields(e, kv...)
	e.Msg(msg)
}

func (z ZeroLogger) Error(msg string, kv ...any) {
	e := z.log.Error()
	addFields(e, kv...)
	e.Msg(msg)
}
