package main

import (
	"context"
	"sync"
)

func runSemaphore(ctx context.Context, in <-chan *CDR) <-chan *CDR {
	sem := make(chan struct{}, 500)
	out := make(chan *CDR)
	go func() {
		defer close(out)
		wg := sync.WaitGroup{}
		for cdr := range in {
			sem <- struct{}{}
			wg.Add(1)
			go func(cdr *CDR) {
				defer wg.Done()
				CalculateDurationF(cdr)
				SetCallDirectionF(cdr)
				LookupRateZoneF(cdr)
				HashAnonymizedIDF(cdr)
				FetchHomeOperatorF(cdr)
				CheckRiskScoreF(cdr)

				out <- cdr
				<-sem
			}(cdr)
		}
		wg.Wait()
	}()
	return out

}
