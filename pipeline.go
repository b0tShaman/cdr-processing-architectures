package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type Stage func(context.Context, <-chan *CDR) <-chan *CDR

var cap = 500

func runPipeline(ctx context.Context, in <-chan *CDR) <-chan *CDR {
	pipeline := []Stage{
		CalculateDuration, // Fast
		SetCallDirection,  // Slow DB
		LookupRateZone,    // Memory
		HashAnonymizedID,  // CPU
		FetchHomeOperator, // Very Slow DB
		CheckRiskScore,    // Network
	}

	for _, stage := range pipeline {
		in = stage(ctx, in) // daisy chain
	}
	return in
}

// ---------------------------------------------------------
// 0. FAST: Calculate Duration
// ---------------------------------------------------------
func CalculateDuration(ctx context.Context, in <-chan *CDR) <-chan *CDR {
	out := make(chan *CDR, cap)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case cdr, ok := <-in:
				if !ok {
					return
				}

				cdr.DurationSec = cdr.EndTime.Sub(cdr.StartTime).Seconds()
				out <- cdr
			}
		}
	}()
	return out
}

// ---------------------------------------------------------
// 1. SLOW DB: Call Direction
// ---------------------------------------------------------
func SetCallDirection(ctx context.Context, in <-chan *CDR) <-chan *CDR {
	out := make(chan *CDR, cap)
	var wg sync.WaitGroup
	workers := 200

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case cdr, ok := <-in:
					if !ok {
						return
					}
					time.Sleep(40 * time.Millisecond) // Simulate slow DB call
					if strings.HasPrefix(cdr.CallerNumber, "44") {
						cdr.CallDirection = "OUTGOING"
					} else {
						cdr.CallDirection = "INCOMING"
					}
					out <- cdr
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// ---------------------------------------------------------
// 2. MEMORY: Rate Zone Lookup
// ---------------------------------------------------------
func LookupRateZone(ctx context.Context, in <-chan *CDR) <-chan *CDR {
	out := make(chan *CDR, cap)
	var wg sync.WaitGroup
	workers := 1

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case cdr, ok := <-in:
					if !ok {
						return
					}

					const payloadSize = 50 * 1024
					payload := make([]byte, payloadSize)

					// Simulate "Memory Bandwidth" (Writing to the memory)
					for k := 0; k < payloadSize; k += 1024 {
						payload[k] = 1
					}
					cdr.RateZone = fmt.Sprintf("MEM-OP-%d", len(payload))
					out <- cdr
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// ---------------------------------------------------------
// 3. VERY SLOW External API: Home Operator
// ---------------------------------------------------------
func FetchHomeOperator(ctx context.Context, in <-chan *CDR) <-chan *CDR {
	out := make(chan *CDR, cap)
	var wg sync.WaitGroup
	workers := 300

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case cdr, ok := <-in:
					if !ok {
						return
					}

					// Simulate Very slow API Call
					time.Sleep(200 * time.Millisecond)
					cdr.HomeOperator = "Vodafone-UK"
					out <- cdr
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// ---------------------------------------------------------
// 4. CPU: Anonymized ID (GDPR)
// ---------------------------------------------------------
func HashAnonymizedID(ctx context.Context, in <-chan *CDR) <-chan *CDR {
	out := make(chan *CDR, cap)
	var wg sync.WaitGroup
	workers := 1

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case cdr, ok := <-in:
					if !ok {
						return
					}

					// Heavy math operation (SHA256)
					data := cdr.CallerNumber + cdr.ReceiverNumber + cdr.CallID
					sum := sha256.Sum256([]byte(data))
					cdr.AnonymizedID = fmt.Sprintf("%x", sum)
					out <- cdr
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// ---------------------------------------------------------
// 5. NETWORK: Risk Score
// ---------------------------------------------------------
func CheckRiskScore(ctx context.Context, in <-chan *CDR) <-chan *CDR {
	out := make(chan *CDR, cap)
	var wg sync.WaitGroup
	workers := 300

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case cdr, ok := <-in:
					if !ok {
						return
					}

					// Simulate variable Network Jitter (20ms to 120ms)
					latency := time.Duration(20 + rand.Intn(100))
					time.Sleep(latency * time.Millisecond)

					cdr.RiskScore = rand.Intn(100)
					out <- cdr
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
