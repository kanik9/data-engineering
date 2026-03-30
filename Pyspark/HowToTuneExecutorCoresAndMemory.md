# Tune Spark Executor Number, Cores, and Memory

## Overview
Tuning Spark's number of executors, executor cores, and executor memory is crucial for improving job performance. The number of cores and executors are two important configuration parameters that significantly impact resource utilization and performance of your Spark application.

For Detail analysis: https://youtu.be/mA96gUESVZc?list=PLWAuYt0wgRcLCtWzUxNg4BjnYlCZNEVth

---

## 1. Spark Executor

An **executor** is a Spark process responsible for executing tasks on a specific node in the cluster. Each executor is assigned:
- A fixed number of cores
- A certain amount of memory

The number of executors determines the level of parallelism at which Spark can process data.

### Key Benefits of Multiple Executors:
- **Better parallelism and resource utilization**: More executors allow for improved parallel processing
- **Independent data processing**: Each executor can work on a subset of data independently, leading to increased processing speed
- **Important balance**: Too many executors can lead to excessive memory usage and increased overhead due to task scheduling

### Advantages:
- More executors provide increased parallelism and ability to process data in parallel
- Each executor can work on a subset of data independently, leading to improved processing speed
- Better resource utilization by distributing the workload across multiple executor processes

### Considerations:
- Allocating too many executors can lead to excessive memory usage and increased overhead due to task scheduling
- Inefficient executor allocation can result in underutilization of cluster resources
- Optimal number depends on: dataset size, computation complexity, and available cluster resources

---

## 2. Spark Cores

The **number of cores** refers to the total number of processing units available on the machines in your Spark cluster. It represents the parallelism level at which Spark can execute tasks. Each core can handle one concurrent task.

### Benefits of Increasing Cores:
- Spark can execute more tasks simultaneously
- Improved overall throughput of your application
- Better utilization of available computational resources

### Considerations:
- Adding too many cores can introduce overhead due to task scheduling and inter-node communication, especially if cluster resources are limited
- Excessive parallelism can introduce overhead impacting performance
- Optimal number depends on: dataset size, complexity of computations, and available cluster resources

---

## 3. Configuring Spark Number of Executors and Cores

Configuring the number of cores and executors depends on several factors:
- Characteristics of your workload
- Available cluster resources
- Specific requirements of your application

### Example Cluster Configuration
Let's assume you have a Spark cluster with:
- **16 nodes**
- **8 cores per node**
- **32 GB of memory per node**
- **Dataset size**: 1 TB with complex computations

**Available Resources:**
- Total cores in the cluster = 16 nodes × 8 cores = **128 cores**
- Total memory in the cluster = 16 nodes × 32 GB = **512 GB**
- Allocating 80% of available resources to Spark = **410 GB**

---

### 3.1. Tiny Executor Configuration

**Configuration:**
- 1 core per executor
- Number of executors per node = 8
- Executor memory = 32/8 = **4GB**

**Calculation:**
- Total memory available for Spark = 80% of 512 GB = 410 GB
- Number of executors = 410 GB / 4 GB ≈ **102 executors**
- Number of executors per node = 102/16 ≈ **6 executors/node**

**Result:** 102 executors, each having 1 core and 4 GB of memory

#### Pros:
1. **Resource Efficiency**: Consumes less memory and fewer CPU cores
2. **Increased Task Isolation**: Each task runs in a more isolated environment, preventing interference between tasks
3. **Task Granularity**: Beneficial for workloads with a large number of small tasks
4. **Precise Resource Allocation**: Spark can allocate resources more precisely

#### Cons:
- **Increased Overhead**: Higher overhead due to increased number of executor processes and task scheduling
- **Limited Parallelism**: Fewer cores limit the level of parallelism
- **Potential Bottlenecks**: A single long-running task can become a bottleneck
- **Memory Overhead**: Multiple executor processes can add up memory overhead

#### When to Use Tiny Executor Configuration:
- ✅ **Multi-tenant clusters** where multiple applications need to share resources
- ✅ **Applications with many small, independent tasks** that complete quickly
- ✅ **Limited memory scenarios** where memory is at a premium
- ✅ **High concurrency requirements** with many parallel jobs
- ✅ **Testing and development environments** with resource constraints
- ✅ **Streaming applications** with small micro-batches
- ❌ **NOT recommended for:** Large-scale batch processing, memory-intensive operations, or shuffle-heavy workloads

---

### 3.2. Fat Executor Configuration

**Configuration:**
- 8 cores per executor (all cores on one executor)
- Number of executors per node = 8/8 = 1
- Executor memory = 32/1 = **32GB**

**Calculation:**
- Total memory available for Spark = 80% of 512 GB = 410 GB
- Number of executors = 410 GB / 32 GB ≈ **12 executors**
- Number of executors per node = 12/16 ≈ **1 executor/node**

**Result:** 16 executors, each having 8 cores and 32 GB of memory

#### Pros:
1. **Increased Parallelism**: More CPU cores and memory per executor, resulting in improved processing speed and throughput
2. **Reduced Overhead**: Fewer executor processes to manage reduces overhead of task scheduling and inter-node communication
3. **Enhanced Data Locality**: Larger executor memory sizes can accommodate more data partitions in memory
4. **Improved Performance for Complex Tasks**: More resources per executor handle complex computations efficiently

#### Cons:
1. **Resource Overallocation**: Can result in overallocation if cluster doesn't have sufficient resources
2. **Reduced Task Isolation**: Fewer executor processes increase chances of resource contention
3. **Longer Startup Times**: Require more resources and have longer startup times
4. **Difficulty in Resource Sharing**: May not be efficient when sharing resources with other applications
5. **HDFS Throughput Bottleneck**: Running with all cores can cause HDFS throughput issues (more than 5 cores per executor not recommended)
6. **Garbage Collection Issues**: Large heap sizes can cause long GC pauses
7. **Single Point of Failure**: If one executor fails, you lose significant resources

#### When to Use Fat Executor Configuration:
- ✅ **Dedicated cluster** with no resource sharing requirements
- ✅ **Memory-intensive operations** like large joins, aggregations, or caching
- ✅ **Applications with low task count** but high computational complexity
- ✅ **Broadcasting large datasets** that need to be replicated across executors
- ✅ **Machine learning workloads** with large model training data
- ✅ **Single large job** running in isolation
- ❌ **NOT recommended for:** Shared clusters, applications requiring high task parallelism, or when HDFS throughput is critical

---

### 3.3. Balanced Executor Configuration (Recommended)

**Databricks recommends 2-5 cores per executor** as the best initial efficient configuration.

**Configuration:**
- 3 cores per executor
- Leaving 1 core per node for daemon processes
- Number of executors per node = (8 - 1)/3 ≈ **2**
- Executor memory = 32/2 = **16GB**

**Calculation:**
- Total memory available for Spark = 80% of 512 GB = 410 GB
- Number of executors = 410 GB / 16 GB ≈ **32 executors**
- Number of executors per node = 32/16 = **2 executors/node**

**Result:** 32 executors, each having 3 cores and 16 GB of memory

#### Pros:
1. **Optimal Resource Utilization**: Evenly distributes resources across the cluster
2. **Reasonable Parallelism**: Strikes a balance between parallelism and resource efficiency
3. **Flexibility for Multiple Workloads**: Accommodates variety of workloads and dataset sizes
4. **Reduced Overhead**: Fewer executor processes than tiny configuration, leading to lower overhead

#### Cons:
1. **Limited Scaling**: May not scale as effectively as other configurations for significantly increased workload
2. **Trade-off in Task Isolation**: May not offer same level of isolation as smaller executor configurations
3. **Task Granularity**: May not offer same level of fine-grained task allocation for large number of small tasks
4. **Complexity in Resource Management**: Maintaining balance across dynamic cluster can be challenging
5. **Not Optimal for Extremes**: May not be ideal for very specific workloads (very small or very large tasks)

#### When to Use Balanced Executor Configuration:
- ✅ **Production workloads** with typical ETL operations (RECOMMENDED)
- ✅ **Mixed workload patterns** with varying task sizes
- ✅ **Batch processing** with reasonable dataset sizes (100GB - 10TB)
- ✅ **SQL queries** with joins, aggregations, and transformations
- ✅ **Data pipelines** with multiple stages
- ✅ **Starting point** when you don't know workload characteristics
- ✅ **Shared clusters** with moderate resource sharing
- ✅ **Most common use cases** - offers best balance of performance and resource utilization
- ✅ **When HDFS throughput matters** (2-5 cores provides optimal I/O)

---

## 4. Configuration Comparison

### Quick Comparison Table

| Configuration | Executors | Cores/Executor | Memory/Executor | Best For |
|---------------|-----------|----------------|-----------------|----------|
| **Tiny** | 102 | 1 | 4 GB | Small tasks, high isolation needs |
| **Fat** | 16 | 8 | 32 GB | Complex computations, large datasets |
| **Balanced** | 32 | 3 | 16 GB | General purpose, recommended starting point |

---

## 4.1. Detailed Advantages and Disadvantages

### Tiny Executor Configuration

#### ✅ Advantages:
1. **Resource Efficiency**: Minimal memory footprint per executor
2. **High Concurrency**: Supports many parallel applications on shared cluster
3. **Task Isolation**: Failures are contained to single executor with minimal impact
4. **Fine-grained Resource Allocation**: Precise resource distribution
5. **Lower GC Overhead**: Smaller heap sizes result in faster garbage collection
6. **Better for Multi-tenancy**: Multiple users can share cluster efficiently
7. **Quick Startup**: Executors start faster with minimal resource requirements
8. **Fault Tolerance**: Losing one executor has minimal impact on overall job

#### ❌ Disadvantages:
1. **High Task Scheduling Overhead**: More executors = more scheduling coordination
2. **Limited Parallelism per Executor**: Only 1 concurrent task per executor
3. **Increased Network Communication**: More executors = more inter-executor communication
4. **Poor Data Locality**: Data may not be co-located with executors
5. **Shuffle Performance**: More network traffic during shuffle operations
6. **Broadcasting Overhead**: Broadcast variables replicated to many executors
7. **Serialization Costs**: More serialization/deserialization operations
8. **JVM Overhead**: Each executor has JVM overhead (metadata, threads, etc.)

---

### Fat Executor Configuration

#### ✅ Advantages:
1. **High Parallelism per Executor**: 8 concurrent tasks per executor
2. **Reduced Network Overhead**: Fewer executors = less inter-executor communication
3. **Better Data Locality**: More data fits in each executor's memory
4. **Efficient Broadcasting**: Fewer replicas of broadcast variables
5. **Lower Scheduling Overhead**: Fewer executors to coordinate
6. **Good for Shuffle-Heavy Operations**: Large buffers for shuffle data
7. **Memory-Intensive Operations**: Ample memory for caching and joins
8. **Reduced JVM Overhead**: Fewer JVM instances across cluster

#### ❌ Disadvantages:
1. **HDFS Throughput Bottleneck**: >5 cores can overwhelm HDFS connections
2. **Large GC Pauses**: Large heap sizes (>32GB) cause long garbage collection pauses
3. **Resource Waste**: If tasks complete early, resources sit idle
4. **Single Point of Failure**: Losing one executor means losing significant resources
5. **Poor Multi-tenancy**: Difficult to share cluster with other applications
6. **Memory Overhead**: JVM overhead becomes significant with large heaps (>32GB)
7. **Task Stragglers**: One slow task can hold up many cores
8. **Inflexible Resource Allocation**: All-or-nothing resource model
9. **Longer Recovery Time**: Executor failures take longer to recover

---

### Balanced Executor Configuration (Optimal)

#### ✅ Advantages:
1. **Optimal HDFS Throughput**: 2-5 cores provides best I/O performance
2. **Reasonable Parallelism**: 3-5 concurrent tasks per executor
3. **Moderate GC Overhead**: Manageable heap sizes with acceptable GC pauses
4. **Good Resource Utilization**: Balance between parallelism and overhead
5. **Flexible for Various Workloads**: Handles most common use cases effectively
6. **Better Fault Tolerance**: Losing one executor has moderate impact
7. **Suitable for Multi-tenancy**: Can share cluster with reasonable efficiency
8. **Lower Task Scheduling Overhead**: Fewer executors than tiny configuration
9. **Good Data Locality**: Reasonable memory per executor for data caching
10. **Industry Best Practice**: Recommended by Databricks and Spark community
11. **Easier to Debug**: Moderate number of executors to monitor
12. **Predictable Performance**: Stable performance across different workloads

#### ❌ Disadvantages:
1. **Not Optimal for Specific Workloads**: May need tuning for edge cases
2. **Requires Some Tuning**: Still needs adjustment based on workload
3. **Middle-ground Trade-offs**: Not perfect for any specific scenario
4. **May Underutilize in Some Cases**: Might not fully use available resources for certain workloads
5. **Requires Understanding**: Need to understand your workload to validate this is appropriate

---

## 4.2. When to Use Which Configuration

### Use Tiny Executors When:
| Scenario | Reason |
|----------|--------|
| **Shared/Multi-tenant Cluster** | Need to run multiple applications simultaneously |
| **Many Small Tasks** | Thousands of quick tasks (< 1 second each) |
| **Limited Memory** | Total cluster memory < 256 GB |
| **Streaming Applications** | Processing small micro-batches continuously |
| **Development/Testing** | Quick iterations with minimal resources |
| **High Fault Tolerance Needed** | Can afford to lose executors without major impact |

**Example Use Case:**
```
- Real-time stream processing with Spark Streaming
- Development and testing environments
- Applications with >10,000 small independent tasks
- Multi-user notebook environments (Jupyter, Zeppelin)
```

---

### Use Fat Executors When:
| Scenario | Reason |
|----------|--------|
| **Dedicated Cluster** | No resource sharing needed |
| **Large Joins/Aggregations** | Need lots of memory for shuffle operations |
| **Caching Large Datasets** | Dataset > 500 GB needs to be cached |
| **ML Training** | Large model training requiring significant memory |
| **Graph Processing** | Graph algorithms needing lots of memory |
| **Single Large Job** | Running one massive job in isolation |
| **Low Task Count** | < 1000 tasks but each is computationally intensive |

**Example Use Case:**
```
- Training large machine learning models
- Graph processing with GraphX
- Large table joins (> 1 TB)
- Caching entire dataset in memory for iterative algorithms
- Complex analytical queries on very large datasets
```

---

### Use Balanced Executors When:
| Scenario | Reason |
|----------|--------|
| **Production ETL Pipelines** | Standard data transformation jobs |
| **SQL Queries** | Mixed queries with joins, aggregations, filters |
| **Batch Processing** | Regular scheduled batch jobs |
| **Unknown Workload** | Starting point when characteristics unknown |
| **Moderate Dataset Size** | 100 GB - 10 TB range |
| **Mixed Operations** | Combination of transformations and actions |
| **Shared Cluster (Moderate)** | Some resource sharing but not extreme |
| **General Purpose** | 80% of typical Spark workloads |

**Example Use Case:**
```
- Daily ETL jobs processing logs or transactions
- Spark SQL analytics workloads
- Data warehouse operations
- Most data engineering pipelines
- Typical production Spark applications
- When you're unsure - this is the safe default!
```

---

## 4.3. Decision Tree for Executor Configuration

```
Start Here
    |
    ├─ Is this a shared cluster with multiple apps?
    |   ├─ YES → Use Tiny Executors
    |   └─ NO → Continue
    |
    ├─ Do you have memory-intensive operations (large joins/caching)?
    |   ├─ YES → Do you have dedicated cluster?
    |   |   ├─ YES → Use Fat Executors
    |   |   └─ NO → Use Balanced Executors (with higher memory)
    |   └─ NO → Continue
    |
    ├─ Is your dataset > 10 TB?
    |   ├─ YES → Use Balanced Executors (tune based on monitoring)
    |   └─ NO → Continue
    |
    ├─ Do you have > 10,000 small tasks?
    |   ├─ YES → Use Tiny Executors
    |   └─ NO → Continue
    |
    └─ Default → Use Balanced Executors (3-5 cores, 2x cores in memory)
```

---

### General Guidelines:

**Number of executors:**
- Should be equal to the number of cores on each node in the cluster
- If there are more cores than nodes, number of executors should equal number of nodes

**Memory per executor:**
- Based on the size of data to be processed by that executor
- Leave some memory available for OS and other processes
- Good starting point: 1GB of memory per executor (minimum)

**Number of partitions:**
- Should be equal to the number of executors for shuffle operations

---

## 5. How to Size Optimal Executors - Step by Step Guide

### 5.1. Understanding the Inputs

Before sizing executors, gather the following information about your cluster:

#### Cluster Resources:
- **Number of nodes** in the cluster
- **Cores per node** (CPU cores available)
- **Memory per node** (Total RAM available)
- **YARN/Mesos/Standalone** (Cluster manager type)

#### Application Requirements:
- **Dataset size** to be processed
- **Type of operations** (memory-intensive, CPU-intensive, shuffle-heavy)
- **Number of partitions** in your data
- **Caching requirements** (do you need to cache data?)

---

### 5.2. The Optimal Sizing Formula

Follow these steps to calculate optimal executor configuration:

#### **Step 1: Reserve Resources for System Processes**

Always reserve resources for OS and daemon processes:

```
Reserved cores per node = 1 core (for NodeManager, DataNode, etc.)
Reserved memory per node = 1-2 GB (OS and system processes)
```

**Formula:**
```
Available cores per node = Total cores - 1
Available memory per node = Total memory - (1 to 2 GB)
```

---

#### **Step 2: Determine Cores Per Executor**

**Rule of Thumb:** Use **3-5 cores per executor** for optimal performance.

**Why 3-5 cores?**
- **< 2 cores**: Limited parallelism, underutilizes resources
- **2-5 cores**: Optimal HDFS throughput and balanced parallelism
- **> 5 cores**: HDFS throughput bottleneck (HDFS has limited threads per executor)

**Recommendation:**
- **For CPU-intensive jobs**: 5 cores per executor
- **For I/O-intensive jobs**: 3-4 cores per executor
- **For balanced workloads**: 4 cores per executor (good starting point)

**Formula:**
```
Cores per executor = 3 to 5 (recommended)
```

---

#### **Step 3: Calculate Executors Per Node**

Determine how many executors fit on each node:

**Formula:**
```
Executors per node = Floor(Available cores per node / Cores per executor)
```

**Example:**
```
If node has 16 cores:
  Available cores = 16 - 1 = 15 cores
  Cores per executor = 5
  Executors per node = Floor(15 / 5) = 3 executors
```

---

#### **Step 4: Calculate Memory Per Executor**

Distribute available memory across executors on each node:

**Formula:**
```
Memory per executor = Floor(Available memory per node / Executors per node)
```

**Important:** Account for memory overhead!

**YARN Memory Overhead:**
```
Memory overhead = Max(384 MB, 10% of executor memory)
```

**Adjusted Formula:**
```
Executor memory = (Available memory per node / Executors per node) - Memory overhead
```

**Example:**
```
If node has 64 GB RAM:
  Available memory = 64 GB - 1 GB (system) = 63 GB
  Executors per node = 3
  Memory per executor = 63 GB / 3 = 21 GB
  
  With overhead (10%):
    Overhead = 21 GB × 0.10 = 2.1 GB
    Final executor memory = 21 GB - 2.1 GB ≈ 19 GB
```

---

#### **Step 5: Calculate Total Number of Executors**

Calculate total executors across the entire cluster:

**Formula:**
```
Total executors = (Number of nodes × Executors per node) - 1
```

**Note:** Subtract 1 for the Application Master (in YARN mode)

**Example:**
```
If cluster has 10 nodes:
  Executors per node = 3
  Total executors = (10 × 3) - 1 = 29 executors
```

---

### 5.3. Complete Sizing Example

Let's work through a complete example:

#### Scenario:
- **Cluster:** 20 nodes
- **Each node:** 32 cores, 128 GB RAM
- **Cluster manager:** YARN
- **Workload:** Balanced (ETL pipeline)

#### Step-by-Step Calculation:

**Step 1: Reserve System Resources**
```
Available cores per node = 32 - 1 = 31 cores
Available memory per node = 128 GB - 1 GB = 127 GB
```

**Step 2: Choose Cores Per Executor**
```
Cores per executor = 5 (for balanced workload)
```

**Step 3: Calculate Executors Per Node**
```
Executors per node = Floor(31 / 5) = Floor(6.2) = 6 executors
```

**Step 4: Calculate Memory Per Executor**
```
Raw memory per executor = 127 GB / 6 = 21.16 GB

Memory overhead (10%) = 21.16 × 0.10 = 2.11 GB

Final executor memory = 21.16 - 2.11 = 19 GB (rounded)
```

**Step 5: Calculate Total Executors**
```
Total executors = (20 nodes × 6 executors) - 1 (for AM)
Total executors = 120 - 1 = 119 executors
```

#### Final Configuration:
```bash
--num-executors 119
--executor-cores 5
--executor-memory 19G
--driver-memory 4G
--conf spark.yarn.executor.memoryOverhead=2G
```

---

### 5.4. Advanced Sizing Considerations

#### A. Memory Overhead Calculation

**YARN automatically adds overhead**, but you can configure it:

```properties
spark.yarn.executor.memoryOverhead = Max(384 MB, 10% of executor-memory)
```

**For memory-intensive operations**, increase overhead:
```properties
spark.yarn.executor.memoryOverhead = 15-20% of executor-memory
```

**Example:**
- Executor memory: 20 GB
- Normal overhead: 2 GB (10%)
- For memory-intensive: 3-4 GB (15-20%)

---

#### B. Adjusting for Different Workload Types

##### **1. CPU-Intensive Workloads** (Complex transformations, UDFs)
```
Cores per executor = 5 (maximize CPU usage)
Memory per executor = Lower (2-3 GB per core)
```

**Example Configuration:**
```bash
--executor-cores 5
--executor-memory 12G  # ~2.4 GB per core
```

##### **2. Memory-Intensive Workloads** (Large joins, aggregations, caching)
```
Cores per executor = 3-4 (fewer cores, more memory per core)
Memory per executor = Higher (4-6 GB per core)
```

**Example Configuration:**
```bash
--executor-cores 4
--executor-memory 20G  # ~5 GB per core
```

##### **3. I/O-Intensive Workloads** (Reading/writing large datasets)
```
Cores per executor = 3-4 (optimize HDFS throughput)
Memory per executor = Moderate (3-4 GB per core)
```

**Example Configuration:**
```bash
--executor-cores 4
--executor-memory 16G  # ~4 GB per core
```

##### **4. Shuffle-Heavy Workloads** (Lots of groupBy, join operations)
```
Cores per executor = 4-5
Memory per executor = Higher (need buffer space)
Increase spark.sql.shuffle.partitions
```

**Example Configuration:**
```bash
--executor-cores 4
--executor-memory 20G
--conf spark.sql.shuffle.partitions=200
--conf spark.shuffle.memoryFraction=0.4
```

---

#### C. Memory Per Core Guidelines

**Rule of Thumb:** Allocate **3-5 GB of memory per core**

| Memory per Core | Use Case |
|-----------------|----------|
| **2-3 GB** | CPU-intensive, simple transformations |
| **3-4 GB** | Balanced workloads, general ETL |
| **4-6 GB** | Memory-intensive, caching, large joins |
| **6-8 GB** | Very memory-intensive, graph processing |

**Formula:**
```
Total executor memory = (Cores per executor × Memory per core) + Overhead
```

**Example:**
```
Cores per executor = 5
Memory per core = 4 GB
Executor memory = (5 × 4) + 10% = 20 GB + 2 GB = 22 GB
```

---

### 5.5. Dynamic Allocation Considerations

Instead of fixed executor count, enable **dynamic allocation**:

```properties
spark.dynamicAllocation.enabled = true
spark.dynamicAllocation.minExecutors = 10
spark.dynamicAllocation.maxExecutors = 100
spark.dynamicAllocation.initialExecutors = 20
```

**Benefits:**
- Automatically scales executors based on workload
- Releases idle executors to save resources
- Better for shared clusters

**Still need to set:**
- `--executor-cores`
- `--executor-memory`

---

### 5.6. Validation and Tuning Checklist

After calculating your configuration, validate:

#### ✅ Check Resource Utilization:
```
Total cores used = (Executors per node × Cores per executor) × Number of nodes
Should be 85-95% of available cores
```

#### ✅ Check Memory Allocation:
```
Total memory used = (Executors per node × Executor memory) × Number of nodes
Should be 80-90% of available memory
```

#### ✅ Verify Parallelism:
```
Total task slots = Total executors × Cores per executor
Should be >= Number of partitions for good parallelism
```

#### ✅ Check for Common Issues:
- [ ] Cores per executor ≤ 5? (HDFS throughput)
- [ ] Executor memory < 32 GB? (GC overhead)
- [ ] Total executors × cores ≈ 85-95% of cluster cores?
- [ ] Memory overhead configured properly?
- [ ] Driver memory sufficient? (Usually 2-8 GB)

---

### 5.7. Real-World Sizing Examples

#### Example 1: Small Cluster (Development)
```
Cluster: 5 nodes, 8 cores, 32 GB RAM per node

Calculation:
- Available cores = 8 - 1 = 7 cores
- Cores per executor = 3
- Executors per node = Floor(7/3) = 2
- Memory per executor = (32-1)/2 = 15.5 GB
- With overhead: 15.5 - 1.5 = 14 GB
- Total executors = (5 × 2) - 1 = 9

Configuration:
--num-executors 9
--executor-cores 3
--executor-memory 14G
```

#### Example 2: Medium Cluster (Production)
```
Cluster: 15 nodes, 24 cores, 96 GB RAM per node

Calculation:
- Available cores = 24 - 1 = 23 cores
- Cores per executor = 5
- Executors per node = Floor(23/5) = 4
- Memory per executor = (96-1)/4 = 23.75 GB
- With overhead: 23.75 - 2.4 = 21 GB
- Total executors = (15 × 4) - 1 = 59

Configuration:
--num-executors 59
--executor-cores 5
--executor-memory 21G
--conf spark.yarn.executor.memoryOverhead=2500M
```

#### Example 3: Large Cluster (Big Data)
```
Cluster: 50 nodes, 32 cores, 256 GB RAM per node

Calculation:
- Available cores = 32 - 1 = 31 cores
- Cores per executor = 5
- Executors per node = Floor(31/5) = 6
- Memory per executor = (256-2)/6 = 42.3 GB
- With overhead: 42.3 - 4.2 = 38 GB
- Total executors = (50 × 6) - 1 = 299

Configuration:
--num-executors 299
--executor-cores 5
--executor-memory 38G
--conf spark.yarn.executor.memoryOverhead=4500M
```

---

### 5.8. Quick Reference Formulas

#### Essential Formulas Summary:

| Parameter | Formula |
|-----------|---------|
| **Available Cores** | `Total cores per node - 1` |
| **Available Memory** | `Total memory per node - 1-2 GB` |
| **Cores per Executor** | `3 to 5` (optimal range) |
| **Executors per Node** | `Floor(Available cores / Cores per executor)` |
| **Memory per Executor** | `(Available memory / Executors per node) - Overhead` |
| **Memory Overhead** | `Max(384 MB, 10% of executor memory)` |
| **Total Executors** | `(Nodes × Executors per node) - 1` |
| **Total Task Slots** | `Total executors × Cores per executor` |

#### Memory per Core Guidelines:
```
Conservative:     3 GB per core
Balanced:         4 GB per core
Memory-intensive: 5-6 GB per core
```

---

### 5.9. Common Sizing Mistakes to Avoid

| ❌ Mistake | ✅ Solution |
|-----------|-----------|
| Using 1 core per executor | Use 3-5 cores for better performance |
| Using >5 cores per executor | Limit to 5 cores to avoid HDFS bottleneck |
| Executor memory > 40 GB | Split into multiple executors to avoid GC issues |
| Not reserving system resources | Always reserve 1 core and 1-2 GB per node |
| Ignoring memory overhead | Add 10-20% overhead for off-heap memory |
| Fixed executors on dynamic cluster | Use dynamic allocation for better resource sharing |
| Same config for all workloads | Adjust based on CPU vs memory intensity |
| Not monitoring resource usage | Check Spark UI for actual utilization and adjust |

---

## 6. Conclusion

In conclusion, Spark's number of executors and cores plays a crucial role in achieving optimal performance and resource utilization.

### Key Takeaways:
- **No one-size-fits-all configuration**: Settings vary based on workload, data size, computational complexity, and cluster resources
- **Balanced configuration (2-5 cores)**: Recommended as starting point
- **Monitor and tune**: Analyze performance metrics, monitor resource utilization, and conduct benchmarking
- **Consider workload characteristics**: Adjust configuration based on your specific needs

### Best Practices:
1. Start with balanced configuration (3 cores per executor)
2. Leave 1 core per node for daemon processes
3. Allocate 80% of available memory to Spark
4. Monitor application performance and adjust accordingly
5. Test different configurations with your actual workload
6. Consider dataset size and computation complexity

---

## 7. Practical Example

### Scenario: Processing 1TB of Log Data

**Cluster Setup:**
- 10 nodes
- 16 cores per node
- 64 GB memory per node

### Step 1: Calculate Available Resources
```
Total cores = 10 nodes × 16 cores = 160 cores
Total memory = 10 nodes × 64 GB = 640 GB
Available for Spark (80%) = 512 GB
```

### Step 2: Determine Executor Configuration

#### Option A: Balanced Configuration (Recommended)
```
Cores per executor = 5 (within 2-5 range)
Reserve 1 core per node for daemon = 16 - 1 = 15 cores available
Executors per node = 15 / 5 = 3
Total executors = 10 nodes × 3 = 30 executors
Memory per executor = 64 GB / 3 ≈ 21 GB

# Leaving some overhead, use 19 GB per executor
```

**Spark Configuration:**
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 30 \
  --executor-cores 5 \
  --executor-memory 19G \
  --driver-memory 4G \
  --conf spark.yarn.executor.memoryOverhead=2G \
  my-spark-app.py
```

#### Option B: For Memory-Intensive Operations
```
Cores per executor = 4
Executors per node = 15 / 4 = 3
Total executors = 10 × 3 = 30 executors
Memory per executor = 64 GB / 3 ≈ 21 GB
```

**Spark Configuration:**
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 30 \
  --executor-cores 4 \
  --executor-memory 20G \
  --driver-memory 8G \
  --conf spark.yarn.executor.memoryOverhead=2G \
  --conf spark.sql.shuffle.partitions=200 \
  my-spark-app.py
```

### Step 3: PySpark Code Example

```python
from pyspark.sql import SparkSession

# Create Spark session with optimized configuration
spark = SparkSession.builder \
    .appName("LogProcessing") \
    .config("spark.executor.instances", "30") \
    .config("spark.executor.cores", "5") \
    .config("spark.executor.memory", "19g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "150") \
    .config("spark.default.parallelism", "150") \
    .getOrCreate()

# Read large log files
logs_df = spark.read.json("hdfs://path/to/logs/*.json")

# Process data
processed_df = logs_df \
    .filter(logs_df.status == "ERROR") \
    .groupBy("date", "service") \
    .count() \
    .orderBy("count", ascending=False)

# Show results
processed_df.show(20)

# Write output
processed_df.write \
    .mode("overwrite") \
    .parquet("hdfs://path/to/output/error_summary")

spark.stop()
```

### Step 4: Monitoring and Tuning

**Check Spark UI (http://driver-node:4040) for:**
- Task execution time
- Data shuffle size
- Memory usage per executor
- Number of failed tasks

**Tune based on observations:**
- If tasks are waiting → Increase number of executors
- If memory errors → Increase executor memory
- If CPU underutilized → Increase cores per executor
- If shuffle is slow → Adjust `spark.sql.shuffle.partitions`

### Performance Comparison Results

| Configuration | Executors | Cores | Memory | Processing Time | Resource Utilization |
|---------------|-----------|-------|--------|-----------------|---------------------|
| Tiny | 150 | 1 | 3 GB | 45 min | 60% |
| Fat | 10 | 15 | 60 GB | 38 min | 75% |
| **Balanced** | **30** | **5** | **19 GB** | **25 min** | **92%** |

**Winner:** Balanced configuration provides best performance with optimal resource utilization.

---

## Reference
- Source: [SparkByExamples - Tune Spark Executor Number, Cores, and Memory](https://sparkbyexamples.com/spark/spark-tune-executor-number-cores-and-memory/)
