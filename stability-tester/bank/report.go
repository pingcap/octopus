package main

import (
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"
)

var (
	tranCount     uint64
	lastTranCount uint64

	tranErr     uint64
	lastTranErr uint64
)

func report(ctx context.Context, interval time.Duration) {
	start := time.Now()
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		elapsed := time.Since(start)
		currencyTranCount := atomic.LoadUint64(&tranCount)
		currencyTranErr := atomic.LoadUint64(&tranErr)
		fmt.Printf("\n\n****************** Summary *********************\n")
		fmt.Printf("Total Transaction: %d \n", currencyTranCount+currencyTranErr)
		fmt.Printf("Exec Time: %s\n", elapsed.String())
		fmt.Printf("tps: %.2f\n", float64(currencyTranCount)/elapsed.Seconds())
		fmt.Printf("eps: %.2f\n", float64(currencyTranErr)/elapsed.Seconds())
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			printMessage(interval.Seconds())
		}
	}
}

func printMessage(seconds float64) {
	currencyTranCount := atomic.LoadUint64(&tranCount)
	currencyTranErr := atomic.LoadUint64(&tranErr)

	fmt.Printf("Total Transaction: %d    tps: %.2f    eps: %.2f\n",
		currencyTranCount+currencyTranErr, float64(currencyTranCount-lastTranCount)/seconds, float64(currencyTranErr-lastTranErr)/seconds)
	lastTranCount = currencyTranCount
	lastTranErr = currencyTranErr
}
