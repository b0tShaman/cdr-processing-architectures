// 1. Fan-Out / Fan-In Architecture
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

func runFanoutFanin(ctx context.Context, in <-chan *CDR) <-chan *CDR {
	return fanIn(ctx, fanOut(ctx, in))
}

func fanOut(ctx context.Context, in <-chan *CDR) []<-chan *CDR {
	const numWorkers = 500

	workerInputs := make([]chan *CDR, numWorkers)
	workerOutputs := make([]<-chan *CDR, 0, numWorkers)

	for i := 0; i < numWorkers; i++ {
		wIn := make(chan *CDR)  // Private input for this worker
		wOut := make(chan *CDR) // Private output for this worker
		workerInputs[i] = wIn
		workerOutputs = append(workerOutputs, wOut)

		go func(input <-chan *CDR, output chan<- *CDR) {
			defer close(output)
			for {
				select {
				case <-ctx.Done():
					return
				case cdr, ok := <-input:
					if !ok {
						return
					}
					ProcessCDRFanout(cdr)
					// Send processed CDR to output channel
					select {
					case output <- cdr:
					case <-ctx.Done():
						return
					}
				}
			}
		}(wIn, wOut)
	}

	// The Dispatcher (The "Fan-Out" Router)
	// This goroutine reads the main stream and distributes it Round-Robin
	go func() {
		defer func() {
			for _, ch := range workerInputs {
				close(ch)
			}
		}()

		workerIndex := 0
		for {
			select {
			case <-ctx.Done():
				return
			case cdr, ok := <-in:
				if !ok {
					return
				}

				// ROUND-ROBIN DISTRIBUTION
				// We are strictly assigning this CDR to a specific worker.
				// If workerInputs[workerIndex] is blocked (worker is busy),
				// this Dispatcher blocks.
				select {
				case workerInputs[workerIndex] <- cdr:
					workerIndex = (workerIndex + 1) % numWorkers
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return workerOutputs
}

func fanIn(ctx context.Context, ins []<-chan *CDR) <-chan *CDR {
	out := make(chan *CDR)
	wg := sync.WaitGroup{}

	for _, in := range ins {
		wg.Add(1)
		go func(ch <-chan *CDR) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case cdr, ok := <-ch:
					if !ok {
						return
					}
					// Send received CDR to output channel
					select {
					case out <- cdr:
					case <-ctx.Done():
						return
					}
				}
			}
		}(in)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func ProcessCDRFanout(cdr *CDR) {
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
