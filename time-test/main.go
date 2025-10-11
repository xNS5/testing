package main

import (
	"fmt"
	"time"
)

func main() {
	now := time.Now().Local().Truncate(12 * time.Hour)

	fmt.Println(now)

}
