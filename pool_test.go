package main

import (
    "context"
    "testing"
)

// Columns:
// [0: Name] - The function name. -14 means the test ran using 14 CPU Cores (GOMAXPROCS).
// [1: Iterations] - The number of times the loop ran (b.N). The higher the number, the faster the function.
// [2: Latency] - Nanoseconds per Operation. This is the average time it took to process one CDR. The lower the number, the better.
// [3: Memory Usage] - Bytes Allocated per Operation (per CDR processed). The lower the number, the better.
// [4: Allocations] - Number of Memory Allocations (per CDR processed). The lower the number, the better.

func BenchmarkPool(b *testing.B) {
    b.ReportAllocs()

    ctx := context.Background()
    input := make(chan *CDR)

    output := runWorkerPool(ctx, input)

    go func() {
        defer close(input)
        for i := 0; i < b.N; i++ {
            input <- GenerateCDR(i)
        }
    }()

    b.ResetTimer()
    for range output {
    }
}