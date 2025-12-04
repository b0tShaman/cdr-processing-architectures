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

// Global Rate Zone Map for Worker Pool
var rateZoneMap map[string]string

func runWorkerPool(ctx context.Context, in <-chan *CDR) <-chan *CDR {
	out := make(chan *CDR)
	numWorkers := 100
	wg := sync.WaitGroup{}

	rateZoneMap = make(map[string]string)
	for i := range 100000 {
		key := fmt.Sprintf("PREFIX-%d", i)
		rateZoneMap[key] = "EU-Zone-1"
	}

	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for cdr := range in {
				select {
				case <-ctx.Done():
					return
				default:
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
	// Simulate quick in-memory lookup
	lookupKey := fmt.Sprintf("PREFIX-%d", rand.Intn(100000))
	if val, ok := rateZoneMap[lookupKey]; ok {
		cdr.RateZone = val
	} else {
		cdr.RateZone = "GLOBAL-DEFAULT"
	}
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
