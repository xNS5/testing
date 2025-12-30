package pool

import (
	"fmt"
	"time"
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

func getPool(config *PoolConfig) (*Pool, error) {

	target := "localhost:5050"

	fmt.Println("Initializing gRPC Pool")

	pool, err := NewPool(target, config)

	if err != nil {
		fmt.Println("Error initializing grpc pool")
	}

	return pool, err
}
