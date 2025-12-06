// 4. Semaphore (Bounded Parallelism) Architecture
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

func runSemaphore(ctx context.Context, in <-chan *CDR) <-chan *CDR {
	sem := make(chan struct{}, 600)
	out := make(chan *CDR)
	go func() {
		defer close(out)
		wg := sync.WaitGroup{}
	Loop:
		for cdr := range in {
			select {
			case <-ctx.Done():
				break Loop
			case sem <- struct{}{}: // Acquire semaphore
				wg.Add(1)
				go func(cdr *CDR) {
					defer func() {
						<-sem // Release semaphore
						wg.Done()
					}()

					ProcessCDRSem(cdr)

					// Send processed CDR to output channel
					select {
					case out <- cdr:
					case <-ctx.Done():
						return
					}
				}(cdr)
			}
		}
		wg.Wait()
	}()
	return out
}

func ProcessCDRSem(cdr *CDR) {
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
