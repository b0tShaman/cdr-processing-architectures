package main

import (
    "context"
    "fmt"
    "math/rand"
    "time"
)

// CDR represents the data flowing through your pipeline
type CDR struct {
    // --- Raw Fields (Ingested) ---
    CallID         string    `json:"call_id"`
    CallerNumber   string    `json:"caller_number"`
    ReceiverNumber string    `json:"receiver_number"`
    StartTime      time.Time `json:"start_time"`
    EndTime        time.Time `json:"end_time"`

    // --- 5 Enriched Fields (The Benchmark Targets) ---
    DurationSec   float64 `json:"duration_sec"`
    CallDirection string  `json:"call_direction"`
    RateZone      string  `json:"rate_zone"`
    HomeOperator  string  `json:"home_operator"`
    AnonymizedID  string  `json:"anonymized_id"`
    RiskScore     int     `json:"risk_score"`
}

func GenerateCDR(i int) *CDR {
    startTime := time.Now().Add(-1 * time.Duration(rand.Intn(60)) * time.Minute)
    callDuration := time.Duration(1+rand.Intn(600)) * time.Second

    // Calculate EndTime based on StartTime + Duration
    endTime := startTime.Add(callDuration)
    return &CDR{
        // RAW FIELDS (Populated at Source)
        CallID:         fmt.Sprintf("UUID-%d", i),
        CallerNumber:   fmt.Sprintf("447%09d", rand.Intn(999999999)), // Random UK Mobile
        ReceiverNumber: fmt.Sprintf("491%09d", rand.Intn(999999999)), // Random German Mobile
        StartTime:      startTime,
        EndTime:        endTime,
    }
}

func main() {
    start := time.Now()
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    source := make(chan *CDR)

    // The Generator (Simulating Kafka Consumer)
    go func() {
        defer close(source)
        for i := range 10000 {
            // Push to pipeline
            source <- GenerateCDR(i)
        }
    }()

    // Run Pipeline
    output := runDaisyPipeline(ctx, source)

    // Consume Output
    count := 0

    for cdr := range output {
        count++
        fmt.Printf("%+v\n", cdr)
    }

    fmt.Printf("\nFinished processing %d CDRs in %v\n", count, time.Since(start))
}