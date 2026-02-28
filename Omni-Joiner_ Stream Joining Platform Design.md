This is a comprehensive technical design document for the **"Omni-Joiner"**—a high-performance, $N$-way stream joining platform. This document is structured to be "LLM-ready" for tools like Cursor or specialized engineering assistants.

# ---

**Technical Design Document: Omni-Joiner Platform**

## **1\. Requirements Specification**

### **1.1 Functional Requirements**

* **N-Way Join:** Coordinate $N$ streams based on a shared Key (Single or Composite).  
* **Dynamic Projection:** User-defined output schema (e.g., Result \= {A.price, B.status, C.timestamp}).  
* **Configurable Windowing:** Define TTLs per stream or per Join-Group.  
* **Egress Policies:** \* *Inner:* Publish only when all $N$ participants arrive.  
  * *Partial (Outer):* Publish available data after a timeout.  
* **Post-Join Delay:** Optional "settle time" before final publication.  
* **Late Arrival Handling:** (Phase 2\) Trigger "Correction" events if data arrives post-timeout.

### **1.2 Non-Functional Requirements**

* **Latency:** $\< 10\\text{ms}$ Join-to-Egress latency (P99).  
* **Scalability:** Horizontal scaling of processing nodes; 1M+ events/day (Targeting $10k+$ RPS).  
* **Strict Ordering:** Optional per-key FIFO processing.  
* **Fault Tolerance:** No state loss on node failure via Centralized KV.

## ---

**2\. High-Level Architecture (HLD)**

The architecture uses a **Centralized State** pattern with a **Distributed Bloom Filter** to minimize unnecessary database I/O.

### **2.1 System Components**

1. **Ingestion Layer:** Kafka/Redpanda clusters.  
2. **Filter Layer (Bloom):** Redis-backed Bloom Filter to track "Seen" keys across the cluster.  
3. **Processing Engine:** Stateless Golang/Rust workers.  
4. **State Store:** ScyllaDB/Aerospike for high-performance, persistent key-value storage.  
5. **Timeout Manager:** A CDC (Change Data Capture) listener on the State Store to handle expired/incomplete joins.

### **2.2 Sequence Diagram: The Join Flow**

Code snippet

sequenceDiagram  
    participant S as Stream N  
    participant P as Processing Worker  
    participant BF as Redis (Bloom Filter)  
    participant DB as ScyllaDB (State)  
    participant E as Egress (Kafka)

    S-\>\>P: Incoming Event (Key: 123\)  
    P-\>\>BF: Check Key: 123  
      
    alt Key Not Found (First Arrival)  
        BF--\>\>P: "New Key"  
        P-\>\>DB: UPSERT Event Data (TTL: 10m)  
        P-\>\>BF: Mark Key: 123 as Seen  
    else Key Found (Potential Match)  
        BF--\>\>P: "Maybe Exists"  
        P-\>\>DB: Atomic Update & Fetch State  
        DB--\>\>P: Current State Object  
          
        alt Join Complete (All N present)  
            P-\>\>P: Apply Projection & Transformation  
            P-\>\>E: Publish Joined Event  
            P-\>\>DB: Delete State (Cleanup)  
        else Join Incomplete  
            P-\>\>P: Log & Wait  
        end  
    end

## ---

**3\. Database & State Design**

To ensure "extremely fast" performance, we avoid SELECT \-\> UPDATE cycles. We use **Atomic Map Updates**.

### **3.1 ScyllaDB Schema**

We represent the Join Group as a single row where each participant stream populates a specific map entry.

SQL

CREATE TABLE omni\_joiner.join\_state (  
    join\_key\_hash blob,  
    join\_key\_raw text,  
    config\_id uuid,  
    \-- Map of StreamID to EventPayload (JSON or Protobuf)  
    participant\_data map\<text, blob\>,   
    arrival\_timestamps map\<text, timestamp\>,  
    is\_completed boolean,  
    PRIMARY KEY (join\_key\_hash)  
) WITH default\_time\_to\_live \= 600; \-- Dynamic TTL based on user config

### **3.2 State Logic**

* **Write Operation:** UPDATE join\_state SET participant\_data\['StreamA'\] \= ?, arrival\_timestamps\['StreamA'\] \= toTimestamp(now()) WHERE join\_key\_hash \= ?  
* **Completion Check:** The query returns the updated map. The worker checks if size(participant\_data) \== N.

## ---

**4\. Technology Stack**

| Layer | Technology | Selection Rationale |
| :---- | :---- | :---- |
| **Language** | **Golang** | Native concurrency (Goroutines) and low-latency garbage collection. |
| **Messaging** | **Redpanda** | Kafka-compatible but faster; zero-copy data path. |
| **State Store** | **ScyllaDB** | NoSQL with C++ core; handles millions of ops/sec with sub-ms latency. |
| **Cache/Filter** | **Redis** | Used for the RedisBloom module to provide a shared, cluster-wide probabilistic check. |
| **Observability** | **Prometheus/Grafana** | Monitoring "State Age" and "Join Success" metrics. |

## ---

**5\. Execution Phases**

### **Phase 1: Core Engine**

* Standardize Composite Key generation (Hashing).  
* Implement the Atomic Update logic in ScyllaDB.  
* Build the Projection Engine (Basic field mapping).

### **Phase 2: Performance Layer**

* Integrate **RedisBloom** for the "Maybe" check to save DB IOPS.  
* Implement the **CDC Listener** for Timeout handling (Partial Egress).  
* Add Support for **Post-Join Delay** using an in-memory timer or Redis-based delayed queue.

### **Phase 3: Reliability & Ordering**

* Implement **Strict Ordering** via partition-affinity at the Consumer level.  
* **Late Arrival Logic:** Logic to "Upsert" already published records in downstream systems.

## ---

**6\. Evaluation & Test Criteria**

### **6.1 Performance Benchmarks**

* **Target:** 50,000 Joined Events per second.  
* **Latency:** Mean $\< 5\\text{ms}$, P99 $\< 15\\text{ms}$ (excluding network transit).  
* **Bloom Efficiency:** False positive rate $\< 1\\%$.

### **6.2 Test Scenarios**

1. **The "Slow Stream" Test:** Stream A arrives at $T=0$, Stream B arrives at $T=9\\text{min}$ (Join should succeed).  
2. **The "Orphan" Test:** Stream A arrives, Stream B never arrives. Verify Partial Egress triggers at $T=10\\text{min}$.  
3. **The "Race Condition" Test:** Simultaneous arrival of Stream A and B for the same key on different nodes. (Verify Atomic Lock/Update in DB).

---

**Would you like me to generate the initial Golang boilerplate for the Processor or the ScyllaDB connection logic to get started?**