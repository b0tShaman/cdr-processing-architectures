// 3. Worker Pool Architecture
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
	const numWorkers = 600

	out := make(chan *CDR)
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
					ProcessCDRPool(cdr)
					// Send processed CDR to output channel
					select {
					case out <- cdr:
					case <-ctx.Done():
						return
					}
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

func ProcessCDRPool(cdr *CDR) {
	// FAST: Calculate Duration
	CalculateDuration := func(cdr *CDR) {
		cdr.DurationSec = cdr.EndTime.Sub(cdr.StartTime).Seconds()
	}

	// SLOW DB: Call Direction
	SetCallDirection := func(cdr *CDR) {
		time.Sleep(40 * time.Millisecond)
		if strings.HasPrefix(cdr.CallerNumber, "44") {
			cdr.CallDirection = "OUTGOING"
		} else {
			cdr.CallDirection = "INCOMING"
		}
	}

	// MEMORY: Payload for Rate Zone Lookup
	LookupRateZone := func(cdr *CDR) {
		const payloadSize = 50 * 1024
		payload := make([]byte, payloadSize)
		for k := 0; k < payloadSize; k += 1024 {
			payload[k] = 1
		}
		cdr.RateZone = fmt.Sprintf("MEM-OP-%d", len(payload))
	}

	// VERY SLOW External API: Home Operator
	FetchHomeOperator := func(cdr *CDR) {
		time.Sleep(200 * time.Millisecond)
		cdr.HomeOperator = "Vodafone-UK"
	}

	// CPU: Anonymized ID (GDPR)
	HashAnonymizedID := func(cdr *CDR) {
		data := cdr.CallerNumber + cdr.ReceiverNumber + cdr.CallID
		sum := sha256.Sum256([]byte(data))
		cdr.AnonymizedID = fmt.Sprintf("%x", sum)
	}

	// NETWORK: Risk Score (with variable latency)
	CheckRiskScore := func(cdr *CDR) {
		latency := time.Duration(20 + rand.Intn(100))
		time.Sleep(latency * time.Millisecond)
		cdr.RiskScore = rand.Intn(100)
	}

	CalculateDuration(cdr)
	SetCallDirection(cdr)
	LookupRateZone(cdr)
	HashAnonymizedID(cdr)
	FetchHomeOperator(cdr)
	CheckRiskScore(cdr)
}
