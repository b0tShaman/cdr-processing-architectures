# CDR Processing Architectures

This project benchmarks popular Go concurrency patterns by simulating a high-throughput Telecommunications use case: processing Call Detail Records (CDRs).

## 🏗 Architectures Compared

### 1. Fan-Out / Fan-In
Splits a chunk of work into multiple independent goroutines (Fan-Out) to process them simultaneously, then merges the results back into a single channel (Fan-In).
* **Structure:** `Input -> [Goroutine 1 ... Goroutine N] -> Aggregator -> Output`
* **Pros:** Maximizes CPU usage; simple to implement.
* **Cons:** Memory usage spikes if the aggregator is slow (Head-of-Line blocking).

### 2. Pipeline (The Daisy Chain)
Data flows through a series of distinct stages connected by channels. Each stage runs in its own set of goroutines, allowing specialized scaling per stage.
* **Structure:** `Stage A (CPU) -> ch -> Stage B (IO) -> ch -> Stage C (Mem)`
* **Pros:** "Backpressure" is handled naturally via channel buffers; easy to scale specific bottlenecks.
* **Cons:** High latency per individual item; high context-switching overhead.

### 3. Worker Pool
A fixed number of concurrent workers pull from a single input channel and execute the entire processing logic sequentially.
* **Structure:** `Input -> [Worker 1..N] -> Output`
* **Pros:** Predictable resource usage; strictly limits maximum concurrency.
* **Cons:** Slow workers can starve the pool if not managed correctly.

### 4. Semaphore (Bounded Parallelism)
Dynamic creation of goroutines is allowed, but a "token" must be acquired from a semaphore (buffered channel) before execution starts.
* **Structure:** `Request -> Acquire Token -> [Spawn Goroutine] -> Release Token -> Output`
* **Pros:** Very low static memory footprint (no idle workers).
* **Cons:** High allocation churn (creating/destroying goroutines per request).

---

## 🚦 The Simulation Logic

To make the benchmark realistic, processing a single CDR involves 6 distinct steps simulating various system stressors found in production systems:

| Stage | Operation | Simulation Type | Stressor |
| :--- | :--- | :--- | :--- |
| **1. Duration** | Time Calculation | Pure Logic | Very Fast |
| **2. Direction** | DB Lookup | `time.Sleep(40ms)` | **Slow IO** |
| **3. Rate Zone** | Data Enrichment | 50KB Allocation | **Memory Bandwidth** |
| **4. Anonymize** | SHA256 Hash | Cryptography | **High CPU** |
| **5. Operator** | External API | `time.Sleep(200ms)` | **Very Slow IO** |
| **6. Risk Score** | Network Check | Jitter (20-120ms) | **Network Latency** |

---

## 🚀 How to Run

### Prerequisites
* Go 1.20+

### Run the Benchmarks
To run the performance comparison using the custom "Rich Benchmark" harness:

```bash
go test -bench=. -benchmem
```
#### To run for a longer duration to stabilize results
```bash
go test -bench=. -benchtime=5s -benchmem
```

### Understanding the Output
The benchmark suite outputs the following columns in order:

| Column | Metric | Description |
| :--- | :--- | :--- |
| **1** | **Name** | The benchmark function name (e.g., `BenchmarkPipeline-14`). The suffix `-14` indicates the number of CPU cores used. |
| **2** | **Iterations** | The number of times the loop ran to get a stable result. Higher is better (indicates stability). |
| **3** | **ns/op** | **Latency:** The average time taken to process ONE single CDR. Lower is better. |
| **4** | **items/s** | **Throughput:** The actual number of CDRs processed per second. Higher is better. |
| **5** | **peak_go** | **Concurrency Reality:** The maximum number of goroutines active simultaneously. Verifies if the pattern scaled to the intended worker count. |
| **6** | **arch_mem_B** | **Static Footprint:** The memory cost (stacks + queues) allocated *just to spin up the architecture* before processing starts. |
| **7** | **B/op** | **Dynamic Memory:** The amount of heap memory allocated per operation *during* processing. Lower is better. |
| **8** | **allocs/op** | **Allocations:** The number of distinct memory allocations per operation. Lower is better. |

### Run the Simulation
To see the pipeline in action processing 10,000 records:
```bash
go run .