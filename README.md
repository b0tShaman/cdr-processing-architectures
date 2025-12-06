# CDR Processing Architectures

This project benchmarks popular Go concurrency patterns by simulating a Telecommunications use case: processing Call Detail Records (CDRs).



## 🏗 Architectures Compared

### 1. Fan-Out / Fan-In
Splits a chunk of work into multiple independent goroutines (Fan-Out) to process them simultaneously, then merges the results back into a single channel (Fan-In).
* **Structure:** `Input -> [Goroutine 1, Goroutine 2 ... Goroutine N] -> Aggregator -> Output`

### 2. Pipeline (The Daisy Chain)
Data flows through a series of distinct stages connected by channels.
* **Structure:** `Stage A (CPU) -> ch -> Stage B (IO) -> ch -> Stage C (Mem)`

### 3. Worker Pool
A fixed number of concurrent workers pull from a single input channel and execute the entire processing logic sequentially.
* **Structure:** `Input -> [Worker 1..N] -> Output`

### 4. Semaphore (Bounded Parallelism)
Dynamic creation of goroutines is allowed, but a "token" must be acquired from a semaphore (buffered channel) before execution starts. This creates a "gatekeeper" effect.
* **Structure:** `Request -> Acquire Token -> [Spawn Goroutine] -> Release Token -> Output`

## 🚦 The Simulation Logic

To make the benchmark realistic, processing a single CDR involves 6 distinct steps simulating various system stressors:

| Stage | Operation | Simulation Type | Stressor |
| :--- | :--- | :--- | :--- |
| **1. Duration** | Time Calculation | Pure Logic | Very Fast |
| **2. Direction** | DB Lookup | `time.Sleep(40ms)` | **Slow IO** |
| **3. Rate Zone** | Cache Lookup | Map Access | **Memory** |
| **4. Anonymize** | SHA256 Hash | Cryptography | **High CPU** |
| **5. Operator** | External API | `time.Sleep(200ms)` | **Very Slow IO** |
| **6. Risk Score** | Network Check | Jitter (20-120ms) | **Network Latency** |

## 🚀 How to Run

### Prerequisites
* Go 1.20+

### Run the Benchmarks
To compare the performance metrics (Latency, Memory, Allocations):
```bash
go test -bench=. -benchmem
```
#### To run for a longer duration to stabilize results
```bash
go test -bench=. -benchtime=5s -benchmem
```
### Run the Simulation
To see the pipeline in action processing 10,000 records:
```bash
go run .