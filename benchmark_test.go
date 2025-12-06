package main

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Columns:
// [Name]       - The function name. -14 means the test ran using 14 CPU Cores.
// [Iterations] - The number of times the loop ran (b.N).
// [ns/op]      - Latency. Average time taken to process one CDR. Lower is better.
// [items/s]    - **Throughput**. The actual number of CDRs processed per second. Higher is better.
// [peak_go]    - **Concurrency Reality**. The maximum active goroutines observed. Verifies if your architecture actually scaled to 600 workers.
// [arch_mem_B] - **Static Footprint**. The "Startup Cost" in bytes. This captures the stack memory used to spin up the workers before processing began.
// [B/op]       - **Dynamic Memory**. Heap bytes allocated per operation (inside the loop). Lower is better.
// [allocs/op]  - **Allocations**. Number of heap allocations per operation. Lower is better.

// -----------------------------------------------------------------------------
// 1. BENCHMARK ENTRY POINTS
// -----------------------------------------------------------------------------

func BenchmarkFanoutFanin(b *testing.B) {
	runRichBenchmark(b, runFanoutFanin)
}

func BenchmarkPipeline(b *testing.B) {
	runRichBenchmark(b, runPipeline)
}

func BenchmarkPool(b *testing.B) {
	runRichBenchmark(b, runWorkerPool)
}

func BenchmarkSemaphore(b *testing.B) {
	runRichBenchmark(b, runSemaphore)
}

// -----------------------------------------------------------------------------
// 2. SHARED BENCHMARK HELPER (The "Rich" Harness)
// -----------------------------------------------------------------------------
func runRichBenchmark(b *testing.B, runner func(context.Context, <-chan *CDR) <-chan *CDR) {
	// Report standard allocs (B/op) inside the loop
	b.ReportAllocs()

	// --- A. MEMORY SNAPSHOT (Before Startup) ---
	runtime.GC() // Clean up garbage to get a fair baseline
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	input := make(chan *CDR)
	var maxGoroutines int64

	// --- B. START ARCHITECTURE ---
	// This spins up your workers.
	// For Pipeline/Pool, this is where the heavy stack allocation happens.
	output := runner(ctx, input)

	// --- C. MEMORY SNAPSHOT (After Startup) ---
	// We capture memory changes caused *only* by spinning up the pattern.
	runtime.ReadMemStats(&m2)

	// Calculate "Static Cost" (mostly stack memory for workers)
	mallocDelta := int64(m2.HeapAlloc) - int64(m1.HeapAlloc)
	stackDelta := int64(m2.StackInuse) - int64(m1.StackInuse)
	totalArchMem := mallocDelta + stackDelta
	if totalArchMem < 0 {
		totalArchMem = 0
	} // Handle rare GC timing quirks

	// --- D. BACKGROUND MONITOR (Concurrency Tracker) ---
	// Spies on the runtime to see how many goroutines are actually alive
	var wgMon sync.WaitGroup
	wgMon.Add(1)
	go func() {
		defer wgMon.Done()
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				current := int64(runtime.NumGoroutine())
				// Atomic Max Check
				for {
					oldMax := atomic.LoadInt64(&maxGoroutines)
					if current <= oldMax {
						break
					}
					if atomic.CompareAndSwapInt64(&maxGoroutines, oldMax, current) {
						break
					}
				}
			}
		}
	}()

	// --- E. DATA GENERATOR ---
	go func() {
		defer close(input)
		for i := 0; i < b.N; i++ {
			input <- GenerateCDR(i)
		}
	}()

	// --- F. EXECUTION LOOP ---
	b.ResetTimer() // Reset timer so setup time isn't counted in ns/op
	start := time.Now()

	receivedCount := 0
	for range output {
		receivedCount++
	}

	elapsed := time.Since(start)

	// --- G. METRICS REPORTING ---
	if receivedCount != b.N {
		b.Fatalf("Data Loss Detected! Sent %d, Received %d", b.N, receivedCount)
	}

	// 1. Throughput: Items Processed per Second
	opsPerSec := float64(b.N) / elapsed.Seconds()
	b.ReportMetric(opsPerSec, "items/s")

	// 2. Concurrency: Peak active goroutines during the run
	b.ReportMetric(float64(atomic.LoadInt64(&maxGoroutines)), "peak_go")

	// 3. Footprint: Memory cost of the architecture itself (Stacks + Queues)
	b.ReportMetric(float64(totalArchMem), "arch_mem_B")
}
