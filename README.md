# AgenticMQ (AgentBroker)

🚀 **High-performance, lightweight message broker designed natively for coordination, rate-limiting, and checkpointing for autonomous AI Agents (LLMs).**

---

## 🎯 The Problem: Why Kafka & RabbitMQ Fail for AI Agents

Traditional message brokers (RabbitMQ, Kafka, SQS) were built for millisecond-scale, deterministic microservices. They break down when applied to LLM agent workflows because:

1. **Ignorance of API Rate Limits (TPM/RPM):** Standard brokers push messages as fast as possible. When agents query LLM providers (OpenAI, Anthropic), they quickly exceed TPM (Tokens Per Minute) and RPM (Requests Per Minute) quotas, triggering API bans and stalling systems.
2. **Synchronous/Short Execution Lifecycles:** Traditional brokers sever connections or trigger consumer timeouts when a task blocks for seconds, minutes, or even hours (e.g., waiting for human verification).
3. **No Dynamic Model Fallback:** Standard brokers route messages statically. If an LLM rate limit is breached or a call fails, they cannot dynamically rewrite the task envelope to route it to a cheaper/faster model (e.g., falling back from `gpt-4o` to `gpt-4o-mini`).

---

## 🏗️ Architecture

```
       +------------------+
       |   Agent Client   |
       +--------+---------+
                |
                | Submit Task (POST /tasks)
                v
  +-----------------------------------------------------------+
  |                   AgenticMQ Broker (Rust)                 |
  |                                                           |
  |  +--------------------+             +------------------+  |
  |  |  Queue Coordinator |             |   Token-Aware    |  |
  |  |                    |             |   Rate Limiter   |  |
  |  |   - gpt-4o Queue   |             |   - TPM Tracking |  |
  |  |   - mini Queue     |<----------->|   - RPM Tracking |  |
  |  +---------+----------+             +------------------+  |
  |            |                                              |
  |            | Checkpoints & Persistence                    |
  |            v                                              |
  |  +--------------------+                                   |
  |  |  Checkpoint Engine |                                   |
  |  |   (JSON Storage)   |                                   |
  |  +---------+----------+                                   |
  +------------|----------------------------------------------+
               |
               | Writes to file
               v
     +-------------------+
     |   Disk Storage    |
     | (.agenticmq_state)|
     +-------------------+
               ^
               | Long Poll & Checkpoint (GET /tasks/poll, POST checkpoint)
               |
       +-------+----------+
       |   Agent Worker   |
       |  (Python SDK)    |
       +------------------+
```

---

## ⚡ Quickstart (3-Step Setup)

### Step 1: Run the Rust Broker
Make sure you have Rust installed. Clone the repository and run:
```bash
cargo run --manifest-path agenticmq-core/Cargo.toml
```
*The broker will start listening on `http://127.0.0.1:8080` and create a `.agenticmq_state` directory for checkpoints.*

### Step 2: Install Python dependencies
Navigate to the Python SDK directory and install it:
```bash
pip install httpx
```

### Step 3: Run the Human-in-the-Loop Example
Run our pre-configured script demonstrating a worker requesting human validation, pausing, and resuming upon authorization:
```bash
python examples/human_in_the_loop.py
```

---

## 🔧 Deep Dive: Key Features

### 1. Token-Aware Rate Limiting
The Rust core parses each message's `token_budget` and tracks moving sliding windows of actual consumption. When a worker polls for a model queue, the broker prevents head-of-line blocking by bypassing tasks that exceed current TPM/RPM limits, choosing instead the first task that fits remaining capacity.

### 2. State Checkpoint Engine (Pause/Resume)
If an agent detects a dangerous action (e.g., executing shell scripts or dropping a database) or needs human feedback, the worker raises a `HumanApprovalRequired` exception. The broker persists the state to disk, generates a secure `x-resume-key`, and flags the task as `Paused`. Re-submitting the task with the header `x-resume-key` immediately restores it to `Pending` for pickup.

### 3. Smart Fallback Handling
If a worker fails execution or times out, the broker inspects the task envelope's `fallback_models`. Instead of failing terminally, the broker updates the target model to the next fallback option (e.g., falling back to `gpt-4o-mini`), adjusts limits, and places it back in the queue.

---

## 🚀 Enterprise Roadmap

*Interested in scaling AgenticMQ for high-volume corporate workloads? Here is our roadmap:*

* **Raft-Based Clustering:** Core engine synchronization using Raft for horizontal scale and active-active broker clusters.
* **Management Console (UI Dashboard):** Visual representation of TPM/RPM windows, token expenditures, queue depths, and a human-in-the-loop console to approve paused tasks.
* **SOC2 Compliance & Auditing:** Complete logs of trace depths, inputs/outputs, and encrypted task states.
