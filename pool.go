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

func runWorkerPool(ctx context.Context, in <-chan *CDR) <-chan *CDR {
	out := make(chan *CDR, cap)
	numWorkers := 500
	wg := sync.WaitGroup{}

	for range numWorkers {
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
					CalculateDurationF(cdr)
					SetCallDirectionF(cdr)
					LookupRateZoneF(cdr)
					HashAnonymizedIDF(cdr)
					FetchHomeOperatorF(cdr)
					CheckRiskScoreF(cdr)

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
// 0. FAST: Calculate Duration
// ---------------------------------------------------------
func CalculateDurationF(cdr *CDR) {
	cdr.DurationSec = cdr.EndTime.Sub(cdr.StartTime).Seconds()
}

// ---------------------------------------------------------
// 1. SLOW DB: Call Direction
// ---------------------------------------------------------
func SetCallDirectionF(cdr *CDR) {
	time.Sleep(40 * time.Millisecond) // Simulate slow DB call
	if strings.HasPrefix(cdr.CallerNumber, "44") {
		cdr.CallDirection = "OUTGOING"
	} else {
		cdr.CallDirection = "INCOMING"
	}
}

// ---------------------------------------------------------
// 2. MEMORY: Rate Zone Lookup
// ---------------------------------------------------------
func LookupRateZoneF(cdr *CDR) {
	const payloadSize = 50 * 1024
	payload := make([]byte, payloadSize)

	// Simulate "Memory Bandwidth" (Writing to the memory)
	for k := 0; k < payloadSize; k += 1024 {
		payload[k] = 1
	}
	cdr.RateZone = fmt.Sprintf("MEM-OP-%d", len(payload))
}

// ---------------------------------------------------------
// 3. VERY SLOW External API: Home Operator
// ---------------------------------------------------------
func FetchHomeOperatorF(cdr *CDR) {
	// Simulate Very slow API Call
	time.Sleep(200 * time.Millisecond)
	cdr.HomeOperator = "Vodafone-UK"
}

// ---------------------------------------------------------
// 4. CPU: Anonymized ID (GDPR)
// ---------------------------------------------------------
func HashAnonymizedIDF(cdr *CDR) {
	// Heavy math operation (SHA256)
	data := cdr.CallerNumber + cdr.ReceiverNumber + cdr.CallID
	sum := sha256.Sum256([]byte(data))
	cdr.AnonymizedID = fmt.Sprintf("%x", sum)
}

// ---------------------------------------------------------
// 5. NETWORK: Risk Score
// ---------------------------------------------------------
func CheckRiskScoreF(cdr *CDR) {
	// Simulate variable Network Jitter (20ms to 120ms)
	latency := time.Duration(20 + rand.Intn(100))
	time.Sleep(latency * time.Millisecond)

	cdr.RiskScore = rand.Intn(100)
}
