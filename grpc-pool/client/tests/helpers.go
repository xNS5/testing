package tests

import "time"

func TotalRetryBackoff(maxAttempts int, initialBackoff, maxBackoff time.Duration, multiplier float64) time.Duration {
	var total time.Duration

	for i := 0; i < maxAttempts-1; i++ {
		backoff := time.Duration(float64(initialBackoff) * pow(multiplier, float64(i)))
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
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
