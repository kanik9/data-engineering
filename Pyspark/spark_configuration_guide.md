# IADP Lookup Framework - Spark Configuration Guide

This document provides detailed descriptions for each Spark configuration parameter used in the IADP Lookup Framework, organized by workload type and usage scenarios. It includes configurations for both batch processing and near real-time streaming applications.

## Table of Contents
- [Non-Streaming Configurations (Batch Processing)](#non-streaming-configurations-batch-processing)
  - [Adaptive Query Execution (AQE)](#adaptive-query-execution-aqe)
  - [File and Storage Optimizations](#file-and-storage-optimizations)
  - [Join Optimizations](#join-optimizations)
  - [Delta Lake Optimizations](#delta-lake-optimizations)
  - [Shuffle Optimizations](#shuffle-optimizations)
  - [Arrow Integration](#arrow-integration)
- [Streaming Configurations (Real-Time Processing)](#streaming-configurations-real-time-processing)
  - [Streaming Core Settings](#streaming-core-settings)
  - [Kafka Integration](#kafka-integration)
  - [State Management](#state-management)
  - [Checkpoint Management](#checkpoint-management)
  - [Delta Streaming](#delta-streaming)
  - [Continuous Processing](#continuous-processing)
- [Cluster Infrastructure Configurations](#cluster-infrastructure-configurations)
  - [Memory Management](#memory-management)
  - [Parallelism and Partitioning](#parallelism-and-partitioning)
  - [Serialization Settings](#serialization-settings)
  - [Network and Timeout Settings](#network-and-timeout-settings)
  - [Compression Settings](#compression-settings)
  - [Task Execution Optimizations](#task-execution-optimizations)
- [Performance Monitoring and Troubleshooting](#performance-monitoring-and-troubleshooting)
  - [Identifying Slow Tasks and Bottlenecks](#identifying-slow-tasks-and-bottlenecks)
  - [Common Performance Issues](#common-performance-issues)
  - [Configuration Tuning Guidelines](#configuration-tuning-guidelines)
  - [Monitoring Tools and Metrics](#monitoring-tools-and-metrics)
- [Environment-Specific Templates](#environment-specific-templates)

---

# Non-Streaming Configurations (Batch Processing)

This section covers Spark configurations primarily used for batch processing, ETL jobs, and traditional data processing workloads.

## Adaptive Query Execution (AQE)

### `spark.serializer`
- **Description**: Specifies the serializer to use for serializing objects that will be sent over the network or need to be cached in serialized form.
- **Default Value**: `org.apache.spark.serializer.JavaSerializer`
- **Recommended Value**: `org.apache.spark.serializer.KryoSerializer`
- **Purpose**: Kryo serializer is faster and more compact than Java serializer, significantly improving performance.
- **Where to Use**: All environments, especially when dealing with large datasets or frequent shuffles.
- **How to Find Best Value**: 
  - Use Kryo for better performance in most cases
  - Monitor serialization time in Spark UI
  - Consider Java serializer only for compatibility issues
- **Supported Values**: 
  - `org.apache.spark.serializer.KryoSerializer` (recommended)
  - `org.apache.spark.serializer.JavaSerializer`

### `spark.kryo.unsafe`
- **Description**: Enables unsafe-based Kryo serialization which is faster but less safe.
- **Default Value**: `false`
- **Recommended Value**: `true`
- **Purpose**: Improves serialization performance by using unsafe operations.
- **Where to Use**: Production environments where performance is critical and data integrity is managed at application level.
- **How to Find Best Value**: 
  - Enable for performance gains
  - Disable if experiencing data corruption issues
  - Monitor for any serialization errors in logs
- **Supported Values**: `true`, `false`

### `spark.serializer.objectStreamReset`
- **Description**: Controls how often the serializer resets its object stream to avoid memory leaks.
- **Default Value**: `100`
- **Recommended Value**: `50-1000` (depending on memory constraints)
- **Purpose**: Prevents memory leaks in long-running applications by resetting object streams.
- **Where to Use**: Long-running Spark applications, streaming jobs.
- **How to Find Best Value**: 
  - Lower values (50-100) for memory-constrained environments
  - Higher values (500-1000) for compute-intensive workloads
  - Monitor GC logs and memory usage patterns
- **Supported Values**: Any positive integer

### `spark.sql.adaptive.enabled`

### `spark.sql.adaptive.enabled`
- **Description**: Enables Adaptive Query Execution, which re-optimizes and re-plans queries based on runtime statistics.
- **Default Value**: `true` (Spark 3.0+)
- **Recommended Value**: `true`
- **Purpose**: Dynamically optimizes queries during execution for better performance.
- **Where to Use**: All environments, especially beneficial for complex queries and joins.
- **How to Find Best Value**: 
  - Always enable unless experiencing specific compatibility issues
  - Monitor query performance improvements in Spark UI
- **Supported Values**: `true`, `false`

### `spark.sql.adaptive.coalescePartitions.enabled`
- **Description**: Enables partition coalescing to reduce the number of small partitions after shuffle operations.
- **Default Value**: `true` (when AQE is enabled)
- **Recommended Value**: `true`
- **Purpose**: Reduces overhead from having too many small partitions, improving performance and reducing task scheduling overhead.
- **Where to Use**: All environments, especially when dealing with skewed data or after filter operations.
- **How to Find Best Value**: 
  - Enable to reduce small partition overhead
  - Monitor partition sizes in Spark UI
- **Supported Values**: `true`, `false`

### `spark.sql.adaptive.coalescePartitions.minPartitionNum`
- **Description**: Minimum number of partitions after coalescing.
- **Default Value**: Default parallelism of the Spark cluster
- **Recommended Value**: `1-200` (based on cluster size)
- **Purpose**: Prevents over-coalescing which could reduce parallelism too much.
- **Where to Use**: Large clusters where you want to maintain minimum parallelism.
- **How to Find Best Value**: 
  - Set to number of cores in cluster / 2-4
  - Monitor task parallelism and execution time
- **Supported Values**: Any positive integer

### `spark.sql.adaptive.coalescePartitions.parallelismFirst`
- **Description**: Prioritizes parallelism over partition size when coalescing partitions.
- **Default Value**: `true`
- **Recommended Value**: `true` for compute-intensive workloads
- **Purpose**: Ensures adequate parallelism is maintained even when partition sizes are small.
- **Where to Use**: Compute-intensive workloads where parallelism is more important than partition size.
- **How to Find Best Value**: 
  - Enable for CPU-intensive operations
  - Disable for I/O intensive operations where larger partitions are better
- **Supported Values**: `true`, `false`

### `spark.sql.adaptive.advisoryPartitionSizeInBytes`
- **Description**: Target partition size for adaptive partition coalescing.
- **Default Value**: `64MB`
- **Recommended Value**: 
  - Development: `64-128MB`
  - Production: `128-512MB`
  - Large datasets: `256-1GB`
- **Purpose**: Controls the target size of partitions after shuffle operations for optimal performance.
- **Where to Use**: 
  - Smaller values for memory-constrained environments
  - Larger values for high-performance environments with abundant memory
- **How to Find Best Value**: 
  - Start with 128MB and adjust based on:
    - Available executor memory (should be 1/4 to 1/8 of executor memory)
    - Network bandwidth (larger partitions reduce network overhead)
    - Processing time per partition (aim for 1-10 seconds per partition)
  - Monitor partition processing times in Spark UI
- **Supported Values**: Any size in bytes (e.g., `64MB`, `256MB`, `1GB`)

### `spark.sql.adaptive.skewJoin.enabled`
- **Description**: Enables skew join optimization to handle data skew in join operations.
- **Default Value**: `true` (when AQE is enabled)
- **Recommended Value**: `true`
- **Purpose**: Automatically detects and handles skewed partitions in joins by splitting large partitions.
- **Where to Use**: When dealing with skewed datasets, especially in join operations.
- **How to Find Best Value**: 
  - Always enable for production workloads
  - Monitor join performance and skew metrics in Spark UI
- **Supported Values**: `true`, `false`

### `spark.sql.adaptive.skewJoin.skewedPartitionFactor`
- **Description**: A partition is considered skewed if its size is larger than this factor multiplied by the median partition size.
- **Default Value**: `5`
- **Recommended Value**: `3-20` (lower for more aggressive skew detection)
- **Purpose**: Determines the threshold for identifying skewed partitions.
- **Where to Use**: Adjust based on the level of skew in your data.
- **How to Find Best Value**: 
  - Decrease (3-5) for more aggressive skew detection in moderately skewed data
  - Increase (10-20) for highly skewed data to avoid over-optimization
  - Monitor skew handling in Spark UI execution details
- **Supported Values**: Any positive number (typically 2-50)

### `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`
- **Description**: Minimum size for a partition to be considered skewed.
- **Default Value**: `256MB`
- **Recommended Value**: `256MB-2GB` (based on cluster capacity)
- **Purpose**: Prevents small partitions from being unnecessarily split for skew handling.
- **Where to Use**: Adjust based on your typical partition sizes and available memory.
- **How to Find Best Value**: 
  - Set to 2-4x your target partition size
  - Monitor skew partition identification in Spark UI
- **Supported Values**: Any size in bytes (e.g., `256MB`, `1GB`, `2GB`)

### `spark.sql.adaptive.localShuffleReader.enabled`
- **Description**: Enables local shuffle reader optimization to reduce network traffic.
- **Default Value**: `true` (when AQE is enabled)
- **Recommended Value**: `true`
- **Purpose**: Optimizes shuffle reads by reading data locally when possible, reducing network I/O.
- **Where to Use**: All environments, especially beneficial in network-constrained environments.
- **How to Find Best Value**: 
  - Always enable unless experiencing compatibility issues
  - Monitor shuffle read metrics in Spark UI
- **Supported Values**: `true`, `false`

### `spark.sql.adaptive.optimizeWrite.enabled`
- **Description**: Enables optimization of output files during write operations to reduce the number of small files.
- **Default Value**: `false`
- **Recommended Value**: `true` (for production workloads)
- **Purpose**: Optimizes file sizes during write operations, reducing the number of small files and improving subsequent read performance.
- **Where to Use**: Production environments where file organization is important, especially with Delta Lake or Parquet formats.
- **How to Find Best Value**: 
  - Enable for better file organization and downstream read performance
  - Monitor file sizes and counts after write operations
  - May increase write time but improves overall system performance
- **Supported Values**: `true`, `false`

### `spark.sql.optimizer.dynamicPartitionPruning.enabled`
- **Description**: Enables dynamic partition pruning optimization to skip irrelevant partitions during query execution.
- **Default Value**: `true` (Spark 3.0+)
- **Recommended Value**: `true`
- **Purpose**: Dynamically prunes partitions based on join conditions and filters, reducing data scanning.
- **Where to Use**: Partitioned tables with joins and filters, especially beneficial for star schema queries.
- **How to Find Best Value**: 
  - Always enable for partitioned data
  - Monitor partition pruning effectiveness in Spark UI SQL tab
  - Most beneficial with dimension table joins on partition keys
- **Supported Values**: `true`, `false`

---

## File and Storage Optimizations

### `spark.sql.files.maxPartitionBytes`
- **Description**: Maximum number of bytes to pack into a single partition when reading files.
- **Default Value**: `128MB`
- **Recommended Value**: 
  - Small files: `134MB` (134217728 bytes)
  - Large files: `256MB-1GB`
  - Memory-constrained: `64-128MB`
- **Purpose**: Controls how much data is processed per partition, affecting parallelism and memory usage.
- **Where to Use**: When reading file-based data sources (Parquet, Delta, CSV, etc.).
- **How to Find Best Value**: 
  - Lower values (64-134MB) increase parallelism for small files
  - Higher values (256MB-1GB) reduce overhead for large files
  - Should fit comfortably in executor memory
  - Monitor partition sizes and processing times in Spark UI
- **Supported Values**: Size in bytes (e.g., `134217728`, `256MB`, `1GB`)

### `spark.sql.files.openCostInBytes`
- **Description**: Estimated cost to open a file, used by the optimizer to decide file grouping during reads.
- **Default Value**: `4MB`
- **Recommended Value**: `256MB` for large datasets with many small files
- **Purpose**: Helps optimizer decide whether to group small files together for processing efficiency.
- **Where to Use**: When dealing with many small files or optimizing file reading patterns.
- **How to Find Best Value**: 
  - Increase (256MB) when dealing with many small files to encourage grouping
  - Keep default for well-organized large files
  - Monitor file reading patterns and task distribution
- **Supported Values**: Size in bytes (e.g., `4MB`, `256MB`)

### `spark.sql.broadcastTimeout`
- **Description**: Timeout for broadcast operations during joins.
- **Default Value**: `300s` (5 minutes)
- **Recommended Value**: `1800s` (30 minutes) for large datasets
- **Purpose**: Prevents broadcast operations from timing out when dealing with large dimension tables.
- **Where to Use**: When using broadcast joins with large dimension tables or slow networks.
- **How to Find Best Value**: 
  - Increase for large broadcast tables or slow networks
  - Monitor broadcast operation times in Spark UI
  - Balance between allowing sufficient time and quick failure detection
- **Supported Values**: Time duration (e.g., `300s`, `1800s`, `30m`)

---

## Delta Lake Optimizations

### `spark.databricks.delta.autoCompact.enabled`
- **Description**: Automatically compacts small files in Delta tables during write operations.
- **Default Value**: `false`
- **Recommended Value**: `true` (Databricks environments)
- **Purpose**: Reduces the number of small files in Delta tables, improving read performance and reducing metadata overhead.
- **Where to Use**: Delta Lake tables with frequent writes, especially streaming or incremental updates.
- **How to Find Best Value**: 
  - Enable for tables with frequent small writes
  - Monitor file sizes and read performance
  - May increase write latency but improves overall performance
- **Supported Values**: `true`, `false`

### `spark.databricks.delta.optimizeWrite.enabled`
- **Description**: Optimizes file sizes during Delta table write operations.
- **Default Value**: `false`
- **Recommended Value**: `true` (Databricks environments)
- **Purpose**: Automatically optimizes file sizes during writes to Delta tables for better performance.
- **Where to Use**: Delta Lake write operations where file optimization is important.
- **How to Find Best Value**: 
  - Enable for better file organization in Delta tables
  - Monitor write performance and resulting file sizes
  - Combines well with auto-compaction for optimal file management
- **Supported Values**: `true`, `false`

---

## Memory Management

### `spark.executor.memory`
- **Description**: Amount of memory allocated to each executor process.
- **Default Value**: `1g`
- **Recommended Value**: 
  - Development: `2-4g`
  - Production: `8-32g`
  - Testing: `1-2g`
- **Purpose**: Determines the memory available for caching, computation, and storage in each executor.
- **Where to Use**: Adjust based on available cluster memory and workload requirements.
- **How to Find Best Value**: 
  - Start with 80% of available memory per executor
  - Monitor memory usage in Spark UI
  - Increase if seeing frequent spills or GC pressure
  - Consider YARN/K8s memory overhead (typically 10% extra)
- **Supported Values**: Memory size (e.g., `4g`, `16g`, `32g`)

### `spark.driver.memory`
- **Description**: Amount of memory allocated to the driver process.
- **Default Value**: `1g`
- **Recommended Value**: 
  - Development: `1-2g`
  - Production: `4-16g`
  - Large datasets: `8-32g`
- **Purpose**: Memory for driver operations including collecting results, managing metadata, and running driver-side computations.
- **Where to Use**: Increase for operations that collect large results or maintain large broadcast variables.
- **How to Find Best Value**: 
  - Start with 2-4g for most workloads
  - Increase if driver is running out of memory
  - Consider the size of broadcast variables and collected results
  - Monitor driver memory usage in Spark UI
- **Supported Values**: Memory size (e.g., `2g`, `8g`, `16g`)

### `spark.driver.maxResultSize`
- **Description**: Maximum size of results that can be returned to the driver from each partition.
- **Default Value**: `1g`
- **Recommended Value**: `1g-8g` (based on driver memory)
- **Purpose**: Prevents driver from running out of memory when collecting large results.
- **Where to Use**: Adjust based on the size of data you need to collect to the driver.
- **How to Find Best Value**: 
  - Set to 25-50% of driver memory
  - Increase if legitimate operations are failing due to result size limits
  - Monitor driver memory usage when collecting results
- **Supported Values**: Memory size or `0` (unlimited, not recommended)

### `spark.executor.memoryFraction`
- **Description**: Fraction of executor memory used for execution and storage (deprecated in Spark 2.0+ but still affects some operations).
- **Default Value**: `0.6`
- **Recommended Value**: `0.6-0.9`
- **Purpose**: Controls how much of executor memory is available for Spark operations vs. user code.
- **Where to Use**: Legacy configurations or when using older Spark versions.
- **How to Find Best Value**: 
  - Increase (0.8-0.9) for Spark-heavy workloads
  - Decrease (0.5-0.6) for user code with high memory usage
  - Modern Spark uses unified memory management, making this less critical
- **Supported Values**: Float between 0.0 and 1.0

### `spark.executor.memoryStorageFraction`
- **Description**: Fraction of execution memory reserved for caching data.
- **Default Value**: `0.5`
- **Recommended Value**: `0.1-0.5`
- **Purpose**: Controls the balance between execution memory and storage (caching) memory.
- **Where to Use**: Adjust based on caching requirements vs. computation needs.
- **How to Find Best Value**: 
  - Decrease (0.1-0.2) for computation-heavy workloads with minimal caching
  - Increase (0.4-0.5) for workloads with extensive caching requirements
  - Monitor memory usage patterns in Spark UI
- **Supported Values**: Float between 0.0 and 1.0

### `spark.executor.memoryOverhead`
- **Description**: Amount of off-heap memory allocated per executor for JVM overhead, native libraries, and Python processes.
- **Default Value**: `max(384MB, 0.1 * executor memory)`
- **Recommended Value**: `10% of executor memory` (minimum 384MB)
- **Purpose**: Provides additional memory buffer for JVM overhead, native operations, and off-heap allocations.
- **Where to Use**: Essential for PySpark, ML workloads, or applications using native libraries.
- **How to Find Best Value**: 
  - Calculate as 10% of executor memory (e.g., 1.6GB for 16GB executor)
  - Increase for PySpark applications or heavy native library usage
  - Monitor for container kills due to memory limit exceeded
  - Essential in containerized environments (YARN, Kubernetes)
- **Supported Values**: Memory size (e.g., `1g`, `2g`, `1600m`)

### `spark.driver.memoryOverhead`
- **Description**: Amount of off-heap memory allocated to the driver for JVM overhead and native operations.
- **Default Value**: `max(384MB, 0.1 * driver memory)`
- **Recommended Value**: `10% of driver memory` (minimum 384MB)
- **Purpose**: Provides memory buffer for driver's JVM overhead, broadcast variables, and result collection.
- **Where to Use**: Important when driver handles large broadcast variables or collects substantial results.
- **How to Find Best Value**: 
  - Calculate as 10% of driver memory
  - Increase if driver experiences memory pressure or container kills
  - Monitor driver memory usage patterns
  - Critical in containerized deployments
- **Supported Values**: Memory size (e.g., `512m`, `1g`, `800m`)

---

## Parallelism and Partitioning

### `spark.default.parallelism`
- **Description**: Default number of partitions in RDDs returned by transformations like join, reduceByKey, and parallelize when not set by user.
- **Default Value**: For distributed shuffle operations, the largest number of partitions in a parent RDD. For operations like parallelize with no parent RDDs, it depends on cluster manager
- **Recommended Value**: 
  - Development: `4-16` (2-4x cores)
  - Production: `32-128` (2-4x total cores)
  - Large clusters: `64-256`
- **Purpose**: Controls the level of parallelism for operations that don't explicitly specify partition count.
- **Where to Use**: Set based on cluster size and workload characteristics.
- **How to Find Best Value**: 
  - Rule of thumb: 2-4 times the number of CPU cores in cluster
  - Increase for I/O intensive workloads
  - Decrease for memory-intensive workloads
  - Monitor task distribution and execution time in Spark UI
- **Supported Values**: Any positive integer

### `spark.sql.shuffle.partitions`
- **Description**: Number of partitions to use when shuffling data for joins or aggregations.
- **Default Value**: `200`
- **Recommended Value**: 
  - Development: `50-200`
  - Production: `400-2000`
  - Large datasets: `800-4000`
  - Auto-tuning: `auto` (Databricks-specific)
- **Purpose**: Controls parallelism and partition size for shuffle operations in SQL queries.
- **Where to Use**: Critical for SQL query performance, especially joins and aggregations.
- **How to Find Best Value**: 
  - Start with data size in GB × 4-8
  - Adjust so each partition processes 100MB-1GB of data
  - Increase for better parallelism, decrease to reduce overhead
  - Use `auto` in Databricks for automatic optimization
  - Monitor shuffle partition sizes and task duration in Spark UI
- **Supported Values**: Any positive integer or `auto` (Databricks)

### `spark.executor.cores`
- **Description**: Number of CPU cores to allocate per executor.
- **Default Value**: `1` in YARN mode, all available cores in standalone mode
- **Recommended Value**: 
  - Development: `1-2`
  - Production: `2-8`
  - Compute-optimized: `4-8`
- **Purpose**: Controls the number of concurrent tasks that can run in each executor.
- **Where to Use**: Balance between parallelism and resource allocation efficiency.
- **How to Find Best Value**: 
  - Consider HDFS throughput (typically optimal at 3-5 cores per executor)
  - Balance with available CPU cores per node
  - More cores = more parallelism but potentially more GC pressure
  - Monitor CPU utilization and task scheduling in cluster manager
- **Supported Values**: Any positive integer (typically 1-8)

---

## Join Optimizations

### `spark.sql.autoBroadcastJoinThreshold`
- **Description**: Maximum size in bytes for a table that will be broadcast to all worker nodes for join operations.
- **Default Value**: `10MB`
- **Recommended Value**: 
  - Development: `50-200MB`
  - Production: `200MB-2GB`
  - Memory-constrained: `50-100MB`
  - Large tables: `-1` (disabled)
- **Purpose**: Enables broadcast joins for small tables, avoiding expensive shuffle operations.
- **Where to Use**: Optimize joins with dimension tables or small lookup tables.
- **How to Find Best Value**: 
  - Set to 5-20% of executor memory for normal use
  - Use `-1` to disable auto-broadcast for very large datasets to prevent memory issues
  - Increase for faster joins with larger dimension tables
  - Decrease if executors run out of memory
  - Monitor broadcast join usage in Spark UI SQL tab
  - Consider network bandwidth for broadcasting large tables
- **Supported Values**: Size in bytes, or `-1` to disable broadcast joins

### `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold`
- **Description**: Threshold for using shuffled hash join instead of sort-merge join for small tables.
- **Default Value**: `0` (disabled)
- **Recommended Value**: `0` (keep disabled unless specific use case)
- **Purpose**: Enables shuffled hash join for small tables that don't qualify for broadcast join.
- **Where to Use**: Rarely used; sort-merge join is typically more efficient.
- **How to Find Best Value**: 
  - Keep at 0 unless experiencing specific performance issues with sort-merge joins
  - If enabled, set to much smaller value than broadcast threshold (e.g., 64MB)
- **Supported Values**: Size in bytes, or `0` to disable

---

## Network and Timeout Settings

### `spark.network.timeout`
- **Description**: Default timeout for all network interactions.
- **Default Value**: `120s`
- **Recommended Value**: 
  - Stable networks: `120-300s`
  - Unstable networks: `600-1200s`
  - Large datasets: `800-1200s`
- **Purpose**: Prevents tasks from hanging indefinitely due to network issues.
- **Where to Use**: Adjust based on network stability and data transfer requirements.
- **How to Find Best Value**: 
  - Increase if seeing frequent network timeout errors
  - Consider network latency and bandwidth
  - Monitor failed tasks due to timeouts in Spark UI
  - Balance between fault tolerance and quick failure detection
- **Supported Values**: Time duration (e.g., `300s`, `10m`)

### `spark.executor.heartbeatInterval`
- **Description**: Interval between executor heartbeats to the driver.
- **Default Value**: `10s`
- **Recommended Value**: `10-120s` (increase for large clusters or unstable networks)
- **Purpose**: Allows driver to detect failed executors and network issues.
- **Where to Use**: Adjust based on cluster stability and network conditions.
- **How to Find Best Value**: 
  - Increase for large clusters to reduce heartbeat overhead
  - Increase for unstable networks to avoid false positives
  - Keep lower for quick failure detection
  - Should be much smaller than network timeout
- **Supported Values**: Time duration (e.g., `30s`, `2m`)

---

## Compression Settings

### `spark.shuffle.compress`
- **Description**: Enables compression of shuffle outputs.
- **Default Value**: `true`
- **Recommended Value**: `true`
- **Purpose**: Reduces disk I/O and network traffic during shuffle operations at the cost of CPU.
- **Where to Use**: Almost always beneficial, especially for network-bound workloads.
- **How to Find Best Value**: 
  - Keep enabled unless CPU is the primary bottleneck
  - Monitor shuffle I/O and network metrics in Spark UI
- **Supported Values**: `true`, `false`

### `spark.shuffle.spill.compress`
- **Description**: Enables compression when spilling data to disk during shuffles.
- **Default Value**: `true`
- **Recommended Value**: `true`
- **Purpose**: Reduces disk space usage and I/O when spilling shuffle data to disk.
- **Where to Use**: Beneficial for disk-constrained environments and large shuffles.
- **How to Find Best Value**: 
  - Keep enabled to save disk space and reduce I/O
  - Monitor spill metrics in Spark UI
- **Supported Values**: `true`, `false`

### `spark.io.compression.codec`
- **Description**: Compression codec used for internal data such as RDD partitions, event log, broadcast variables and shuffle outputs.
- **Default Value**: `lz4`
- **Recommended Value**: 
  - General use: `lz4` (fast compression/decompression)
  - Storage-constrained: `snappy` (good balance)
  - CPU-abundant: `zstd` (best compression ratio)
- **Purpose**: Balances compression ratio, CPU usage, and compression/decompression speed.
- **Where to Use**: Choose based on whether CPU, network, or storage is the bottleneck.
- **How to Find Best Value**: 
  - `lz4`: Fastest, lower compression ratio
  - `snappy`: Good balance of speed and compression
  - `zstd`: Best compression, more CPU intensive
  - Monitor CPU usage and I/O patterns
- **Supported Values**: `lz4`, `snappy`, `zstd`, `lzf`

### `spark.shuffle.spill`
- **Description**: Enables spilling of shuffle data to disk when memory is insufficient.
- **Default Value**: `true`
- **Recommended Value**: `true`
- **Purpose**: Prevents out-of-memory errors by spilling data to disk during large shuffles.
- **Where to Use**: Should always be enabled to ensure fault tolerance.
- **How to Find Best Value**: 
  - Always keep enabled
  - Monitor spill frequency and size in Spark UI
  - If excessive spilling, consider increasing executor memory
- **Supported Values**: `true`, `false`

---

## Shuffle Optimizations

### `spark.shuffle.reduceLocality.enabled`
- **Description**: Enables locality-aware reduce task scheduling for shuffle operations.
- **Default Value**: `true`
- **Recommended Value**: 
  - Small/medium clusters: `true`
  - Large clusters with high network bandwidth: `false`
- **Purpose**: Attempts to schedule reduce tasks on nodes that have shuffle data locally.
- **Where to Use**: Beneficial when network bandwidth is limited compared to local disk I/O.
- **How to Find Best Value**: 
  - Enable for network-constrained environments
  - Disable for high-bandwidth networks where scheduling flexibility is more important
  - Monitor shuffle read locality in Spark UI
- **Supported Values**: `true`, `false`

---

## Task Execution Optimizations

### `spark.speculation`
- **Description**: Enables speculative execution of tasks when Spark detects that some tasks are running slower than expected (stragglers). When enabled, Spark launches duplicate tasks for slow-running tasks and uses the result from whichever task completes first.
- **Default Value**: `false`
- **Recommended Value**: 
  - **Batch ETL jobs with heterogeneous clusters**: `true`
  - **Streaming/Real-time applications**: `false` (NOT recommended)
  - **Memory-constrained environments**: `false`
  - **Homogeneous, stable clusters**: `false`
- **Purpose**: Mitigates the impact of slow tasks (stragglers) that can bottleneck job completion, improving overall job performance in environments with variable task execution times.
- **Where to Use**: 
  - ✅ **Best for**: Large batch processing jobs on heterogeneous clusters with variable node performance
  - ✅ **Good for**: ETL pipelines with significant data skew or resource contention
  - ❌ **Avoid for**: Streaming applications, real-time processing, exactly-once semantics requirements
  - ❌ **Not suitable for**: Memory-constrained environments, homogeneous clusters with consistent performance
- **How to Find Best Value**: 
  - **Enable when**: Experiencing frequent slow tasks in heterogeneous environments
  - **Keep disabled when**: Running streaming jobs, using state management, or resource-constrained environments
  - Monitor task duration variance in Spark UI - high variance indicates potential benefit
  - Watch for excessive resource usage from duplicate tasks
  - Consider network bandwidth impact of running duplicate tasks
- **Performance Trade-offs**:
  - ✅ **Benefits**: Reduces job completion time when stragglers are present, improves tail latency
  - ❌ **Costs**: Increases resource consumption (CPU, memory, network), may cause resource contention
- **When NOT to Use**:
  - **Streaming Applications**: Can interfere with exactly-once processing guarantees
  - **State Management**: Duplicate tasks can cause conflicts with state stores and checkpoints
  - **Resource-Constrained Clusters**: Extra resource usage may worsen overall performance
  - **Consistent Performance Environments**: Overhead outweighs benefits when task times are predictable
- **Supported Values**: `true`, `false`

### `spark.speculation.interval`
- **Description**: How often Spark checks for speculative tasks (only relevant when speculation is enabled).
- **Default Value**: `100ms`
- **Recommended Value**: 
  - **Fast detection**: `100ms-500ms`
  - **Conservative detection**: `1s-5s`
  - **Large clusters**: `1s-10s`
- **Purpose**: Controls the frequency of straggler detection - shorter intervals provide faster detection but more overhead.
- **Where to Use**: Fine-tune speculation responsiveness when speculation is enabled.
- **How to Find Best Value**: 
  - Decrease for faster straggler detection in time-critical jobs
  - Increase for large clusters to reduce monitoring overhead
  - Balance between detection speed and system overhead
- **Supported Values**: Time duration (e.g., `500ms`, `1s`, `5s`)

### `spark.speculation.quantile`
- **Description**: Fraction of tasks that must complete before speculation is enabled for remaining tasks.
- **Default Value**: `0.75` (75% of tasks must complete)
- **Recommended Value**: 
  - **Aggressive speculation**: `0.5-0.6` (50-60%)
  - **Conservative speculation**: `0.8-0.9` (80-90%)
  - **Balanced approach**: `0.75` (75%)
- **Purpose**: Prevents premature speculation by waiting until enough tasks complete to establish performance baseline.
- **Where to Use**: Adjust based on how aggressively you want to handle stragglers.
- **How to Find Best Value**: 
  - Lower values (0.5-0.6) for more aggressive straggler handling
  - Higher values (0.8-0.9) for conservative approach to avoid unnecessary duplication
  - Monitor task completion patterns and speculation effectiveness
- **Supported Values**: Float between 0.0 and 1.0

### `spark.speculation.multiplier`
- **Description**: How much slower a task must be than the median to be considered for speculation.
- **Default Value**: `1.5` (task must be 1.5x slower than median)
- **Recommended Value**: 
  - **Sensitive detection**: `1.2-1.3`
  - **Moderate detection**: `1.5-2.0`
  - **Conservative detection**: `2.0-3.0`
- **Purpose**: Defines the threshold for identifying slow tasks - lower values are more sensitive to performance differences.
- **Where to Use**: Tune based on acceptable variance in task execution times.
- **How to Find Best Value**: 
  - Decrease (1.2-1.3) for environments with low task time variance
  - Increase (2.0-3.0) for environments where some variation is expected
  - Monitor speculation trigger frequency and effectiveness
- **Supported Values**: Float greater than 1.0 (typically 1.1-5.0)

### `spark.speculation.task.duration.threshold`
- **Description**: Minimum task duration before a task becomes eligible for speculation.
- **Default Value**: `100ms`
- **Recommended Value**: 
  - **Short tasks**: `100ms-1s`
  - **Medium tasks**: `1s-10s`
  - **Long-running tasks**: `10s-60s`
- **Purpose**: Prevents speculation of very short tasks where the overhead of launching duplicate tasks exceeds the benefit.
- **Where to Use**: Set based on typical task duration in your workload.
- **How to Find Best Value**: 
  - Set to 5-10% of average task duration
  - Higher thresholds for workloads with naturally short tasks
  - Monitor average task durations in Spark UI
- **Supported Values**: Time duration (e.g., `1s`, `10s`, `30s`)

---

# Streaming Configurations (Real-Time Processing)

This section covers Spark configurations specifically designed for streaming applications, real-time data processing, and near real-time analytics.

## Streaming Core Settings

### `spark.streaming.backpressure.enabled`
- **Description**: Enables automatic rate limiting (backpressure) for streaming applications to prevent system overload.
- **Default Value**: `true` (Spark 2.0+)
- **Recommended Value**: `true` (always enable)
- **Purpose**: Automatically adjusts the rate of data ingestion based on processing capacity to prevent out-of-memory errors and system overload.
- **Where to Use**: All streaming applications, especially those with variable data rates or processing times.
- **How to Find Best Value**: 
  - Always enable for production streaming applications
  - Monitor streaming metrics for rate adjustments
  - Works in conjunction with rate limiting parameters
- **Supported Values**: `true`, `false`

### `spark.streaming.receiver.writeAheadLog.enable`
- **Description**: Enables write-ahead logs for streaming receivers to ensure fault tolerance.
- **Default Value**: `false`
- **Recommended Value**: `true` (for production streaming apps)
- **Purpose**: Provides fault tolerance by storing received data to reliable storage before processing.
- **Where to Use**: Production streaming applications requiring exactly-once or at-least-once processing guarantees.
- **How to Find Best Value**: 
  - Enable for critical streaming applications
  - May increase latency but ensures data durability
  - Monitor checkpoint storage usage
- **Supported Values**: `true`, `false`

### `spark.streaming.blockInterval`
- **Description**: Interval at which streaming data is batched into blocks before processing.
- **Default Value**: `200ms`
- **Recommended Value**: 
  - **High-throughput production**: `200ms`
  - **Low-latency requirements**: `100ms-200ms`
  - **Development/testing**: `500ms-1000ms`
- **Purpose**: Controls the granularity of streaming data processing and affects both latency and throughput.
- **Where to Use**: Fine-tune based on latency requirements and throughput needs.
- **How to Find Best Value**: 
  - Shorter intervals = lower latency but more overhead
  - Longer intervals = higher throughput but increased latency
  - Monitor streaming UI for batch processing times
- **Supported Values**: Time duration (e.g., `100ms`, `200ms`, `500ms`)

### `spark.sql.streaming.metricsEnabled`
- **Description**: Enables detailed metrics collection for structured streaming queries.
- **Default Value**: `false`
- **Recommended Value**: `true` (for monitoring and debugging)
- **Purpose**: Provides detailed metrics about streaming query performance, including input rates, processing times, and watermark progression.
- **Where to Use**: All streaming applications for monitoring and performance analysis.
- **How to Find Best Value**: 
  - Always enable for production streaming applications
  - Essential for monitoring streaming performance
  - Minimal performance overhead
- **Supported Values**: `true`, `false`

### `spark.sql.streaming.ui.enabled`
- **Description**: Enables the streaming tab in Spark UI for monitoring streaming queries.
- **Default Value**: `true`
- **Recommended Value**: `true`
- **Purpose**: Provides web UI for monitoring streaming queries, batch progress, and performance metrics.
- **Where to Use**: Development and production environments for streaming query monitoring.
- **How to Find Best Value**: 
  - Keep enabled for visibility into streaming performance
  - Essential for debugging streaming issues
- **Supported Values**: `true`, `false`

### `spark.sql.streaming.numRecentProgressUpdates`
- **Description**: Number of recent progress updates to keep in memory for streaming queries.
- **Default Value**: `100`
- **Recommended Value**: 
  - **Development/testing**: `100-200`
  - **Production monitoring**: `500-1000`
  - **Memory-constrained**: `50-100`
- **Purpose**: Controls how many progress updates are retained for monitoring and debugging purposes.
- **Where to Use**: Adjust based on monitoring needs and memory availability.
- **How to Find Best Value**: 
  - Increase for detailed monitoring in production
  - Decrease to reduce memory usage
  - Monitor streaming UI responsiveness
- **Supported Values**: Any positive integer

## Kafka Integration

### `spark.streaming.kafka.maxRatePerPartition`
- **Description**: Maximum rate (records per second) at which data will be read from each Kafka partition.
- **Default Value**: Not set (unlimited)
- **Recommended Value**: 
  - **Development/testing**: `1000-5000`
  - **Production**: `10000-50000`
  - **High-volume production**: `50000-100000`
- **Purpose**: Controls the ingestion rate from Kafka to prevent overwhelming downstream processing.
- **Where to Use**: Kafka-based streaming applications requiring rate limiting.
- **How to Find Best Value**: 
  - Start conservative and increase based on processing capacity
  - Monitor lag and processing times
  - Balance between throughput and system stability
  - Consider downstream system capacity
- **Supported Values**: Any positive integer (records per second)

### `spark.streaming.receiver.maxRate`
- **Description**: Maximum rate (records per second) at which streaming receivers will consume data.
- **Default Value**: Not set (unlimited)
- **Recommended Value**: 
  - **Development/testing**: `5000-10000`
  - **Production**: `50000-100000`
  - **Memory-constrained**: `1000-5000`
- **Purpose**: Prevents receivers from consuming data faster than the system can process it.
- **Where to Use**: Receiver-based streaming applications (legacy streaming API).
- **How to Find Best Value**: 
  - Set based on processing capacity and available resources
  - Monitor receiver performance and system resource usage
  - Consider memory and CPU constraints
- **Supported Values**: Any positive integer (records per second)

## State Management

### `spark.sql.streaming.stateStore.providerClass`
- **Description**: Specifies the state store provider implementation for stateful streaming operations.
- **Default Value**: `org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider`
- **Recommended Value**: `org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider` (default is optimal)
- **Purpose**: Manages state data for stateful streaming operations like aggregations and joins.
- **Where to Use**: Stateful streaming applications with aggregations, windowing, or stream-stream joins.
- **How to Find Best Value**: 
  - Use default HDFS-backed provider for most cases
  - Consider custom providers for specific requirements
  - Monitor state store performance and size
- **Supported Values**: Full class name of state store provider

### `spark.sql.streaming.stateStore.maintenanceInterval`
- **Description**: Interval for performing maintenance operations on state stores (cleanup, compaction).
- **Default Value**: `60s`
- **Recommended Value**: `600s` (10 minutes) for production
- **Purpose**: Controls how frequently state stores are cleaned up and optimized.
- **Where to Use**: Long-running streaming applications with stateful operations.
- **How to Find Best Value**: 
  - Increase for production to reduce maintenance overhead
  - Decrease if state store grows rapidly
  - Monitor state store size and performance
- **Supported Values**: Time duration (e.g., `300s`, `600s`, `10m`)

### `spark.sql.streaming.flatMapGroupsWithState.stateTimeout`
- **Description**: Default timeout mode for stateful operations in structured streaming.
- **Default Value**: `NoTimeout`
- **Recommended Value**: `ProcessingTimeTimeout` for most use cases
- **Purpose**: Controls how state timeout is handled in stateful streaming operations.
- **Where to Use**: Streaming applications using flatMapGroupsWithState or mapGroupsWithState.
- **How to Find Best Value**: 
  - Use `ProcessingTimeTimeout` for time-based state expiration
  - Use `EventTimeTimeout` for event-time based expiration
  - Monitor state store growth
- **Supported Values**: `NoTimeout`, `ProcessingTimeTimeout`, `EventTimeTimeout`

## Checkpoint Management

### `spark.sql.streaming.checkpointLocation.deleteTmpCheckpoints`
- **Description**: Automatically deletes temporary checkpoint files to prevent accumulation.
- **Default Value**: `true`
- **Recommended Value**: `true`
- **Purpose**: Prevents temporary checkpoint files from accumulating and consuming storage space.
- **Where to Use**: All streaming applications with checkpointing enabled.
- **How to Find Best Value**: 
  - Always enable to prevent checkpoint storage bloat
  - Monitor checkpoint directory size
- **Supported Values**: `true`, `false`

### `spark.sql.streaming.forceDeleteTempCheckpointLocation`
- **Description**: Forces deletion of temporary checkpoint locations on startup.
- **Default Value**: `false`
- **Recommended Value**: `true` for development, `false` for production
- **Purpose**: Cleans up temporary checkpoint locations that may interfere with streaming query startup.
- **Where to Use**: Development environments or when dealing with checkpoint corruption issues.
- **How to Find Best Value**: 
  - Enable in development for clean restarts
  - Use cautiously in production to avoid data loss
- **Supported Values**: `true`, `false`

### `spark.sql.streaming.stopGracefullyOnShutdown`
- **Description**: Enables graceful shutdown of streaming queries when the application terminates.
- **Default Value**: `false`
- **Recommended Value**: `true`
- **Purpose**: Ensures streaming queries complete current batches and update checkpoints before shutdown.
- **Where to Use**: Production streaming applications requiring clean shutdown procedures.
- **How to Find Best Value**: 
  - Enable for production applications to prevent data loss
  - May increase shutdown time but ensures data integrity
- **Supported Values**: `true`, `false`

## Delta Streaming

### `spark.databricks.delta.streaming.allowSourceColumnRename`
- **Description**: Allows column renames in Delta streaming sources without failing the stream.
- **Default Value**: `false`
- **Recommended Value**: `true` for flexible schema evolution
- **Purpose**: Enables schema evolution in streaming Delta sources by allowing column renames.
- **Where to Use**: Delta Lake streaming applications requiring schema flexibility.
- **How to Find Best Value**: 
  - Enable if schema evolution is expected
  - Monitor for unintended schema changes
  - Consider data consistency implications
- **Supported Values**: `true`, `false`

### `spark.databricks.delta.streaming.allowSourceSchemaEvolution`
- **Description**: Enables schema evolution in Delta streaming sources.
- **Default Value**: `false`
- **Recommended Value**: `true` for dynamic schema requirements
- **Purpose**: Allows streaming queries to adapt to schema changes in Delta sources.
- **Where to Use**: Streaming applications reading from Delta tables with evolving schemas.
- **How to Find Best Value**: 
  - Enable for applications expecting schema changes
  - Test thoroughly with schema evolution scenarios
  - Monitor for data quality impacts
- **Supported Values**: `true`, `false`

## Continuous Processing

### `spark.sql.streaming.continuous.executorIdleTimeout`
- **Description**: Timeout for idle executors in continuous processing mode.
- **Default Value**: `60s`
- **Recommended Value**: `120s` for production
- **Purpose**: Controls when idle executors are terminated in continuous processing to free up resources.
- **Where to Use**: Continuous processing streaming applications.
- **How to Find Best Value**: 
  - Increase for production to avoid frequent executor restarts
  - Balance between resource usage and responsiveness
  - Monitor executor lifecycle in streaming UI
- **Supported Values**: Time duration (e.g., `60s`, `120s`, `5m`)

### `spark.sql.streaming.continuous.epochBacklogQueueSize`
- **Description**: Size of the backlog queue for continuous processing epochs.
- **Default Value**: `2000`
- **Recommended Value**: `2000-5000` based on memory availability
- **Purpose**: Controls the buffer size for continuous processing to handle temporary processing slowdowns.
- **Where to Use**: Continuous processing applications with variable processing rates.
- **How to Find Best Value**: 
  - Increase for better buffering during processing spikes
  - Monitor memory usage and queue utilization
  - Balance between memory usage and resilience
- **Supported Values**: Any positive integer

---

# Cluster Infrastructure Configurations

This section covers Spark configurations that affect cluster-wide behavior, resource management, and fundamental Spark operations across both streaming and batch workloads.

## Memory Management

### `spark.sql.execution.arrow.pyspark.enabled`
- **Description**: Enables Apache Arrow-based columnar data transfers between Spark and Python/Pandas.
- **Default Value**: `false`
- **Recommended Value**: 
  - PySpark with Pandas operations: `true`
  - Pure Spark SQL: `false`
  - Memory-constrained: `false`
- **Purpose**: Significantly improves performance for PySpark operations involving Pandas DataFrames.
- **Where to Use**: When using PySpark with Pandas UDFs, toPandas(), or other Pandas operations.
- **How to Find Best Value**: 
  - Enable if using Pandas operations in PySpark
  - Disable if not using Pandas or experiencing memory issues
  - Monitor memory usage and operation performance
- **Supported Values**: `true`, `false`

### `spark.sql.execution.arrow.maxRecordsPerBatch`
- **Description**: Maximum number of records in each Arrow batch when transferring data.
- **Default Value**: `10000`
- **Recommended Value**: 
  - Small memory: `5000-10000`
  - Large memory: `50000-200000`
  - Development: `10000`
- **Purpose**: Controls memory usage and transfer efficiency for Arrow operations.
- **Where to Use**: Fine-tune when using Arrow with PySpark for optimal memory usage.
- **How to Find Best Value**: 
  - Increase for better throughput with abundant memory
  - Decrease if experiencing memory pressure
  - Monitor Arrow operation performance and memory usage
- **Supported Values**: Any positive integer (typically 1000-500000)

### `spark.sql.columnVector.offheap.enabled`
- **Description**: Enables off-heap memory for columnar vectors in memory-optimized scenarios.
- **Default Value**: `false`
- **Recommended Value**: `true` for memory-constrained environments
- **Purpose**: Reduces on-heap memory pressure by using off-heap memory for columnar data.
- **Where to Use**: Memory-constrained environments where GC pressure is high.
- **How to Find Best Value**: 
  - Enable if experiencing frequent GC or memory pressure
  - Monitor GC metrics and memory usage patterns
  - May have slight performance overhead but reduces memory pressure
- **Supported Values**: `true`, `false`

---

## Environment-Specific Recommendations

### Development Environment
**Characteristics**: Limited resources, quick iteration, debugging focus
- **Memory**: Conservative settings (2-4g executor, 1-2g driver)
- **Parallelism**: Low to moderate (4-16 tasks)
- **Partitions**: Smaller partition sizes (64-128MB)
- **Features**: Enable most optimizations, disable Arrow if not needed

### Testing Environment  
**Characteristics**: Minimal resources, functional testing focus
- **Memory**: Minimal settings (1-2g executor, 1g driver)
- **Parallelism**: Very low (4-8 tasks)
- **Partitions**: Small partitions (50-100)
- **Features**: Disable expensive features like Arrow

### Production Environment
**Characteristics**: High performance, large datasets, stability focus
- **Memory**: Generous settings (8-32g executor, 4-16g driver) with 10% overhead
- **Parallelism**: High (64-512 tasks) with adaptive optimization
- **Partitions**: Optimized for throughput (400-2000 shuffle partitions or auto)
- **Features**: Enable all performance optimizations (AQE, skew handling, write optimization)
- **File Management**: Enable Delta optimizations and dynamic partition pruning
- **Broadcast**: Increase timeout (1800s) and consider disabling auto-broadcast for large tables

### Large Dataset Configuration
**Characteristics**: 100M+ records, memory and compute intensive
- **Memory**: Maximum available (16-64g executor, 8-32g driver)
- **Parallelism**: Very high (128-512 tasks)
- **Partitions**: Large partitions (512MB-2GB advisory size)
- **Features**: Aggressive optimizations, advanced skew handling

### Memory-Optimized Configuration
**Characteristics**: Limited memory, need to prevent OOM errors
- **Memory**: Conservative with aggressive spilling
- **Parallelism**: Balanced to avoid memory pressure
- **Partitions**: Smaller partitions to fit in memory
- **Features**: Disable memory-intensive features, enable off-heap storage

### Compute-Optimized Configuration
**Characteristics**: CPU-intensive workloads, abundant compute resources
- **Memory**: Balanced memory allocation
- **Parallelism**: Maximum parallelism (96+ tasks)
- **Partitions**: Optimized for CPU utilization
- **Features**: Enable CPU-intensive optimizations like Arrow with large batch sizes

---

## Recommended Configuration Templates

### Large-Scale Production Configuration
```ini
# Core optimizations
spark.serializer = org.apache.spark.serializer.KryoSerializer
spark.sql.adaptive.enabled = true
spark.sql.adaptive.optimizeWrite.enabled = true
spark.sql.optimizer.dynamicPartitionPruning.enabled = true

# Memory settings (adjust based on cluster)
spark.executor.memory = 16g
spark.executor.memoryOverhead = 1600m  # 10% of executor memory
spark.driver.memory = 8g
spark.driver.memoryOverhead = 800m  # 10% of driver memory
spark.driver.maxResultSize = 4g

# Parallelism and partitioning
spark.sql.shuffle.partitions = 512  # or "auto" in Databricks
spark.default.parallelism = 512
spark.sql.files.maxPartitionBytes = 134217728  # 128MB

# File and join optimizations
spark.sql.files.openCostInBytes = 256MB
spark.sql.broadcastTimeout = 1800s
spark.sql.autoBroadcastJoinThreshold = -1  # Disable for large datasets

# Skew handling
spark.sql.adaptive.skewJoin.enabled = true
spark.sql.adaptive.skewJoin.skewedPartitionFactor = 3

# Delta Lake optimizations (Databricks)
spark.databricks.delta.autoCompact.enabled = true
spark.databricks.delta.optimizeWrite.enabled = true

# Arrow integration (if using PySpark)
spark.sql.execution.arrow.pyspark.enabled = true
```

### Memory-Optimized Configuration
```ini
# Prioritize memory efficiency
spark.executor.memory = 8g
spark.executor.memoryOverhead = 800m
spark.executor.memoryFraction = 0.6
spark.executor.memoryStorageFraction = 0.3

# Smaller partitions for memory constraints
spark.sql.files.maxPartitionBytes = 134217728  # 128MB
spark.sql.adaptive.advisoryPartitionSizeInBytes = 64MB
spark.sql.shuffle.partitions = 400

# Disable memory-intensive features
spark.sql.execution.arrow.pyspark.enabled = false
spark.sql.autoBroadcastJoinThreshold = 50MB

# Enable off-heap storage
spark.sql.columnVector.offheap.enabled = true
```

---

## Performance Tuning Guidelines

### 1. Memory Tuning
- Monitor GC time in Spark UI (should be < 10% of task time)
- Watch for spill metrics (minimize but don't eliminate completely)
- Adjust executor memory based on data size and operations
- Use off-heap storage for memory-constrained environments

### 2. Partition Tuning
- Target 100MB-1GB per partition for most workloads
- Ensure 2-4 tasks per CPU core for good utilization
- Monitor task duration (aim for 1-10 seconds per task)
- Use AQE to automatically optimize partition sizes

### 3. Join Optimization
- Use broadcast joins for tables < 20% of executor memory
- Enable AQE skew join handling for skewed datasets
- Monitor join strategies in Spark UI SQL tab
- Consider bucketing for frequently joined tables

### 4. Shuffle Optimization
- Minimize shuffle operations through good data organization
- Use appropriate compression codecs based on CPU/network trade-offs
- Monitor shuffle read/write metrics in Spark UI
- Consider push-based shuffle for large-scale workloads

### 5. Network Optimization
- Increase timeouts for unstable networks or large data transfers
- Use compression to reduce network traffic
- Monitor network I/O in cluster metrics
- Consider locality settings based on network bandwidth

---

# Performance Monitoring and Troubleshooting

This section provides comprehensive guidance on identifying performance issues, monitoring slow tasks, and using Spark configurations to resolve common problems.

## Identifying Slow Tasks and Bottlenecks

### Using Spark UI for Performance Analysis

#### 1. **Jobs Tab - Overall Job Performance**
- **Look for**: Long-running jobs, failed jobs, skewed task execution times
- **Key Metrics**: Job duration, number of stages, active/completed tasks
- **Red Flags**: Jobs running much longer than expected, high failure rates

#### 2. **Stages Tab - Stage-Level Analysis**
- **Look for**: Stages with long duration, high task failure rates, data skew
- **Key Metrics**: 
  - Median task duration vs. max task duration (large difference indicates skew)
  - Shuffle read/write sizes
  - Input/output data sizes
  - GC time percentage
- **Red Flags**: 
  - Task duration variance > 3x median (indicates data skew)
  - GC time > 10% of task time (memory pressure)
  - Excessive shuffle spill (memory insufficient)

#### 3. **Tasks Tab - Task-Level Debugging**
- **Look for**: Individual slow tasks, failed tasks, resource usage patterns
- **Key Metrics**: Task duration, GC time, shuffle metrics, input/output sizes
- **Red Flags**: 
  - Tasks taking 10x longer than median (stragglers)
  - High memory usage or frequent GC
  - Large shuffle spill sizes

#### 4. **Executors Tab - Resource Utilization**
- **Look for**: Memory usage patterns, executor failures, resource imbalance
- **Key Metrics**: Memory used/available, active tasks, completed tasks, failed tasks
- **Red Flags**: 
  - Consistently high memory usage (> 80%)
  - Frequent executor failures
  - Uneven task distribution across executors

### Streaming-Specific Monitoring

#### 5. **Streaming Tab - Real-Time Performance**
- **Look for**: Processing delays, input rates vs. processing rates, batch durations
- **Key Metrics**: 
  - Input rate (records/sec)
  - Processing time per batch
  - Scheduling delay
  - Total delay
- **Red Flags**: 
  - Processing time > batch interval (falling behind)
  - Increasing scheduling delay (system overload)
  - High input rate with low processing rate

## Common Performance Issues

### 1. **Data Skew Issues**

#### **Symptoms:**
- Few tasks taking much longer than others
- Some executors idle while others are overloaded
- Large variance in task duration (max >> median)

#### **Detection using Spark Configurations:**
```python
# Enable skew join detection
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")  # Reduce for more sensitive detection
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Monitor partition sizes
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")  # Smaller partitions for better distribution
```

#### **Solutions:**
- **Salting**: Add random prefix to skewed keys
- **Broadcast Joins**: For small dimension tables
- **Custom Partitioning**: Use `repartition()` with custom logic
- **Pre-aggregation**: Reduce data before joins

### 2. **Memory Pressure and OOM Errors**

#### **Symptoms:**
- Executor failures with OutOfMemoryError
- High GC time (> 10% of task time)
- Frequent shuffle spill to disk
- Tasks killed by container managers

#### **Detection and Configuration:**
```python
# Monitor memory usage patterns
spark.conf.set("spark.executor.memory", "16g")  # Increase if insufficient
spark.conf.set("spark.executor.memoryOverhead", "1600m")  # 10% of executor memory
spark.conf.set("spark.driver.memoryOverhead", "800m")  # 10% of driver memory

# Enable off-heap storage for memory-constrained environments
spark.conf.set("spark.sql.columnVector.offheap.enabled", "true")

# Adjust partition sizes to fit in memory
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")  # Smaller partitions
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
```

#### **Solutions:**
- **Increase Executor Memory**: Add more memory per executor
- **Optimize Partition Sizes**: Reduce partition size to fit in memory
- **Enable Compression**: Reduce memory footprint
- **Tune GC Settings**: Use G1GC for large heaps
- **Use Off-heap Storage**: For columnar operations

### 3. **Slow Shuffle Operations**

#### **Symptoms:**
- High shuffle read/write times
- Network bottlenecks
- Large amounts of shuffle spill
- Tasks spending most time on shuffle operations

#### **Detection and Configuration:**
```python
# Optimize shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "400")  # Adjust based on data size
spark.conf.set("spark.default.parallelism", "400")

# Enable compression for shuffles
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
spark.conf.set("spark.io.compression.codec", "lz4")  # Fast compression

# Optimize serialization
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

#### **Solutions:**
- **Optimize Partition Count**: Target 100MB-1GB per partition
- **Use Broadcast Joins**: For small tables to avoid shuffle
- **Enable Push-based Shuffle**: For large-scale workloads
- **Optimize Join Strategies**: Use bucketing for frequently joined tables

### 4. **Streaming Lag and Backpressure Issues**

#### **Symptoms:**
- Processing time > batch interval
- Increasing input queue size
- Scheduling delays
- Memory pressure in streaming applications

#### **Detection and Configuration:**
```python
# Enable backpressure control
spark.conf.set("spark.streaming.backpressure.enabled", "true")

# Set appropriate rates for different environments
# Development
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")
spark.conf.set("spark.streaming.receiver.maxRate", "5000")

# Production  
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "10000")
spark.conf.set("spark.streaming.receiver.maxRate", "50000")

# Optimize block intervals
spark.conf.set("spark.streaming.blockInterval", "200ms")  # Production
spark.conf.set("spark.streaming.blockInterval", "500ms")  # Development

# Enable monitoring
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "500")
```

#### **Solutions:**
- **Adjust Input Rates**: Use backpressure and rate limiting
- **Optimize Processing Logic**: Reduce per-record processing time
- **Scale Resources**: Add more executors or increase memory
- **Optimize State Operations**: Use efficient state management

### 5. **Task Startup and Scheduling Overhead**

#### **Symptoms:**
- Many short-lived tasks (< 100ms)
- High task scheduling overhead
- Poor resource utilization

#### **Detection and Configuration:**
```python
# Optimize task granularity
spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")  # Larger partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB")

# Avoid speculation for short tasks
spark.conf.set("spark.speculation", "false")  # Disable for consistent environments
spark.conf.set("spark.speculation.task.duration.threshold", "10s")  # Only speculate long tasks
```

#### **Solutions:**
- **Increase Partition Sizes**: Reduce number of small tasks
- **Optimize File Organization**: Use larger files where possible
- **Disable Speculation**: For homogeneous clusters
- **Tune Parallelism**: Reduce over-parallelization

## Configuration Tuning Guidelines

### Performance Tuning Workflow

#### 1. **Baseline Measurement**
```python
# Enable comprehensive monitoring
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")  # For streaming
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

#### 2. **Memory Optimization**
```python
# Start with conservative memory settings
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "800m")  # 10%
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.driver.memoryOverhead", "400m")  # 10%

# Monitor and adjust based on usage patterns
# If high GC time: Increase executor memory
# If memory waste: Decrease executor memory or increase executor count
```

#### 3. **Partition Optimization**
```python
# Start with data-size based partitioning
data_size_gb = 100  # Your dataset size in GB
target_partition_size_mb = 128  # Target size in MB
optimal_partitions = (data_size_gb * 1024) // target_partition_size_mb

spark.conf.set("spark.sql.shuffle.partitions", str(optimal_partitions))
spark.conf.set("spark.sql.files.maxPartitionBytes", f"{target_partition_size_mb}MB")
```

#### 4. **Environment-Specific Tuning**

##### **Development Environment**
```python
# Conservative settings for quick iteration
spark.conf.set("spark.executor.memory", "2g")
spark.conf.set("spark.driver.memory", "1g")
spark.conf.set("spark.sql.shuffle.partitions", "50")
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")

# Streaming settings
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")
spark.conf.set("spark.streaming.blockInterval", "500ms")
```

##### **Production Environment**
```python
# Optimized for performance and reliability
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.executor.memoryOverhead", "1600m")
spark.conf.set("spark.driver.memory", "8g")
spark.conf.set("spark.driver.memoryOverhead", "800m")
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")

# Production streaming settings
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "10000")
spark.conf.set("spark.streaming.blockInterval", "200ms")
spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "500")

# Enable all optimizations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
```

## Monitoring Tools and Metrics

### 1. **Spark UI Metrics**
- **Jobs Tab**: Overall job performance and failure rates
- **Stages Tab**: Stage-level metrics and task distribution
- **Storage Tab**: RDD/DataFrame caching efficiency
- **Environment Tab**: Configuration verification
- **Executors Tab**: Resource utilization and executor health
- **SQL Tab**: Query plans and optimization decisions
- **Streaming Tab**: Real-time streaming metrics (for streaming apps)

### 2. **Key Performance Indicators (KPIs)**

#### **Batch Processing KPIs:**
- **Job Duration**: Total time from start to completion
- **Task Duration Distribution**: Median, 75th percentile, max task times
- **GC Time Ratio**: GC time / Total task time (should be < 10%)
- **Shuffle Metrics**: Read/write sizes and spill amounts
- **Memory Utilization**: Peak memory usage vs. allocated memory
- **CPU Utilization**: Task time vs. wall-clock time

#### **Streaming Processing KPIs:**
- **Processing Rate**: Records processed per second
- **Input Rate**: Records received per second  
- **Batch Duration**: Time to process each batch
- **Scheduling Delay**: Time between batch ready and start processing
- **Total Delay**: End-to-end latency from input to output
- **Memory Usage**: Streaming memory consumption patterns

### 3. **Automated Monitoring Setup**

#### **Configuration for Comprehensive Monitoring:**
```python
# Enable all monitoring features
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
spark.conf.set("spark.sql.streaming.ui.enabled", "true") 
spark.conf.set("spark.eventLog.enabled", "true")
spark.conf.set("spark.eventLog.dir", "/path/to/spark-events")

# Streaming-specific monitoring
spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "500")
spark.conf.set("spark.streaming.ui.retainedBatches", "1000")
```

#### **Alerting Thresholds:**
- **High GC Time**: GC time > 10% of task time
- **Memory Pressure**: Memory usage > 85% consistently
- **Task Skew**: Max task duration > 3x median task duration
- **Streaming Lag**: Processing time > 1.5x batch interval
- **High Failure Rate**: Task failure rate > 5%

### 4. **Log Analysis for Troubleshooting**

#### **Key Log Patterns to Monitor:**
- `OutOfMemoryError`: Memory configuration issues
- `java.lang.InterruptedException`: Task cancellation/timeout issues  
- `org.apache.spark.shuffle.FetchFailedException`: Shuffle/network issues
- `Container killed by YARN`: Resource limit exceeded
- `Task failed`: Individual task failures requiring investigation

#### **Useful Log Configurations:**
```python
# Enable detailed logging for debugging
spark.conf.set("spark.sql.execution.debug.maxToStringFields", "100")
spark.conf.set("spark.sql.adaptive.logLevel", "INFO")
```

---

# Environment-Specific Templates

## Near Real-Time Streaming Template

Based on the provided configuration, here's the complete template for near real-time streaming applications:

### **Development Environment**
```python
# Environment-specific configurations
if env != "prod":
    # File and storage optimizations
    spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")
    spark.conf.set("spark.sql.files.openCostInBytes", "1MB") 
    spark.conf.set("spark.sql.broadcastTimeout", "300")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
    
    # Streaming rate limiting
    spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")
    spark.conf.set("spark.streaming.receiver.maxRate", "5000")
    spark.conf.set("spark.streaming.blockInterval", "500ms")
    spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "100")
```

### **Production Environment**
```python
else:  # Production environment
    # File and storage optimizations for large datasets
    spark.conf.set("spark.sql.files.maxPartitionBytes", "500MB")
    spark.conf.set("spark.sql.files.openCostInBytes", "4MB")
    spark.conf.set("spark.sql.broadcastTimeout", "1800")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable auto-broadcast
    
    # High-throughput streaming settings
    spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "10000")
    spark.conf.set("spark.streaming.receiver.maxRate", "50000")  
    spark.conf.set("spark.streaming.blockInterval", "200ms")
    spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "500")
```

### **Common Configurations (All Environments)**
```python
# Non-streaming optimizations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Streaming optimizations
spark.conf.set("spark.streaming.backpressure.enabled", "true")
spark.conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
spark.conf.set("spark.sql.streaming.checkpointLocation.deleteTmpCheckpoints", "true")
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
spark.conf.set("spark.sql.streaming.ui.enabled", "true")
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "600s")
spark.conf.set("spark.sql.streaming.flatMapGroupsWithState.stateTimeout", "ProcessingTimeTimeout")
spark.conf.set("spark.sql.streaming.stopGracefullyOnShutdown", "true")
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
spark.conf.set("spark.databricks.delta.streaming.allowSourceColumnRename", "true")
spark.conf.set("spark.databricks.delta.streaming.allowSourceSchemaEvolution", "true")
spark.conf.set("spark.sql.streaming.continuous.executorIdleTimeout", "120s")
spark.conf.set("spark.sql.streaming.continuous.epochBacklogQueueSize", "2000")
```

---

*This guide serves as a comprehensive reference for optimizing Spark configurations in the IADP Lookup Framework. Regular monitoring and iterative tuning based on actual workload patterns will yield the best performance results.*
