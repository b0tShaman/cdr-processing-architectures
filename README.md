# CDR Processing Architectures

This project benchmarks two popular Go concurrency patterns—**The Pipeline (Daisy Chain)** and **The Worker Pool**—by simulating a high-volume Telecommunications use case: processing Call Detail Records (CDRs).

The goal is to determine which architecture yields higher throughput and lower latency when facing a realistic mix of CPU-bound, Memory-bound, and IO-bound tasks.



## 🏗 Architectures Compared

### 1. The Daisy Chain (Pipeline)
Data flows through a series of distinct stages connected by channels.
* **Structure:** `Stage A (CPU) -> ch -> Stage B (IO) -> ch -> Stage C (Mem)`
* **Pros:** Strict separation of concerns; natural backpressure.
* **Cons:** Throughput is capped by the slowest stage.

### 2. The Worker Pool
A fixed number of concurrent workers pull from a single input channel and execute the entire processing logic sequentially.
* **Structure:** `Input -> [Worker 1..N] -> Output`
* **Pros:** High throughput for IO-heavy tasks; simple implementation.
* **Cons:** Resource contention (locking) can occur; harder to debug.

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
# Run benchmarks
# MacOS
go test -bench=. -benchmem
# Windows
go test -bench="." -benchmem

# To run for a longer duration to stabilize results
go test -bench=. -benchtime=5s -benchmem
```
### Run the Simulation
To see the pipeline in action processing 10,000 records:
```bash
go run .