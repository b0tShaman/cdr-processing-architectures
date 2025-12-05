package main

import (
	"context"
	"sync"
)

func runFanoutFanin(ctx context.Context, in <-chan *CDR) <-chan *CDR {
	return fanIn(ctx, fanOut(ctx, in))
}

func fanOut(ctx context.Context, in <-chan *CDR) []<-chan *CDR {
	outs := make([]<-chan *CDR, 0)
	for range 5 {
		out := make(chan *CDR)
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
		outs = append(outs, out)
	}
	return outs
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
					out <- cdr
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
