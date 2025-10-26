# PySpark Learning Resources 🚀

This folder contains comprehensive guides and resources for learning and mastering Apache Spark with Python, focusing on performance optimization, query plan analysis, and configuration management.

## 📁 Contents

### 📚 Documentation Files

| File | Description | Level |
|------|-------------|-------|
| [`HowToReadSparkQueryPlans.md`](./HowToReadSparkQueryPlans.md) | **Complete guide to understanding and analyzing Spark query plans** | Beginner to Expert |
| [`spark_configuration_guide.md`](./spark_configuration_guide.md) | **Comprehensive Spark configuration reference for IADP Lookup Framework** | Intermediate to Advanced |
| [`HowToReadSparkDAG.md`](./HowToReadSparkDAG.md) | Guide to understanding Spark DAGs *(placeholder for future content)* | Intermediate |

### 🖼️ Visual Resources

| File | Description |
|------|-------------|
| [`resources/spark_query_plan_flow.png`](./resources/spark_query_plan_flow.png) | Visual diagram of Spark query execution flow |

## 🎯 Learning Path

### 1. **Beginner Level**
Start here if you're new to Spark performance optimization:
- Begin with the **Query Plans Guide** - [Introduction & Prerequisites](./HowToReadSparkQueryPlans.md#1-introduction--prerequisites-📖)
- Understand the [Query Execution Pipeline](./HowToReadSparkQueryPlans.md#2-fundamentals-query-execution-pipeline-📋)
- Learn [Basic Physical Plan Reading](./HowToReadSparkQueryPlans.md#3-practical-analysis-📖)

### 2. **Intermediate Level**  
Once you understand the basics:
- Deep dive into [Physical Plans](./HowToReadSparkQueryPlans.md#3-understanding-physical-plans-🏗️)
- Study [Catalyst Optimizer](./HowToReadSparkQueryPlans.md#5-catalyst-optimizer-deep-dive-🧠)
- Explore basic [Spark Configurations](./spark_configuration_guide.md#non-streaming-configurations-batch-processing)

### 3. **Advanced Level**
For performance experts:
- Master [Advanced Optimization Techniques](./HowToReadSparkQueryPlans.md#6-advanced-optimization-techniques-🎯)
- Learn [Performance Troubleshooting](./HowToReadSparkQueryPlans.md#7-performance-troubleshooting-🔧)
- Study [Environment-Specific Configurations](./spark_configuration_guide.md#environment-specific-templates)

### 4. **Expert Level**
For Spark specialists:
- Implement [Custom Performance Monitoring](./spark_configuration_guide.md#performance-monitoring-and-troubleshooting)
- Design [Streaming Configurations](./spark_configuration_guide.md#streaming-configurations-real-time-processing)
- Create production-ready optimization strategies

## 🔧 Key Topics Covered

### Query Plan Analysis 📊
- **8-Phase Execution Pipeline**: From SQL parsing to RDD operations
- **Physical Plan Reading**: Bottom-up approach with real examples
- **Performance Bottleneck Identification**: Using Spark UI effectively
- **Optimization Verification**: Confirming Catalyst optimizations work

### Spark Configuration Mastery ⚙️
- **Adaptive Query Execution (AQE)**: Runtime query optimization
- **Memory Management**: Executor and driver memory tuning
- **Join Optimizations**: Broadcast joins and skew handling
- **Streaming Configurations**: Real-time processing setups
- **Environment-Specific Templates**: Dev, test, and production configs

### Performance Optimization 🚀
- **Data Skew Handling**: Detection and mitigation strategies
- **Memory Pressure Solutions**: GC tuning and partition optimization
- **Shuffle Optimization**: Reducing network overhead
- **Streaming Performance**: Backpressure and rate limiting

## 🛠️ Quick Start Guide

### Essential Spark Configurations (Copy-Paste Ready)

```python
# 🚨 QUICK PERFORMANCE FIXES
# Enable Adaptive Query Execution (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Optimize Shuffle Partitions  
cores = sc.defaultParallelism
spark.conf.set("spark.sql.shuffle.partitions", str(cores * 2))

# Enable Cost-Based Optimizer
spark.conf.set("spark.sql.cbo.enabled", "true")
# Don't forget: ANALYZE TABLE your_table COMPUTE STATISTICS

# Increase Broadcast Threshold
spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "50MB")

# Enable Predicate Pushdown
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
```

### Reading Query Plans - Essential Commands

```python
# View different plan stages
df.explain(mode="simple")     # Basic physical plan
df.explain(mode="extended")   # Logical + Physical plans
df.explain(mode="formatted")  # Pretty formatted output

# Enable Cost-Based Optimization
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.sql("ANALYZE TABLE users COMPUTE STATISTICS")
```

## 📋 Performance Checklist

Use this checklist when optimizing Spark applications:

```
🔍 PHYSICAL PLAN ANALYSIS CHECKLIST

□ Start reading from bottom (data sources)
□ Identify Exchange operations (potential bottlenecks)  
□ Check for PushedFilters (predicate pushdown success)
□ Look for BroadcastExchange (join optimization)
□ Count shuffle operations (minimize for performance)
□ Verify projection pushdown (only required columns)
□ Check partition counts (200 default, adjust as needed)
□ Identify skewed partitions (uneven data distribution)
□ Monitor memory usage (avoid spills)
□ Validate join strategies (broadcast > sort-merge > nested-loop)
```

## 🏗️ Environment-Specific Configurations

### Development Environment
```python
# Conservative settings for quick iteration
spark.conf.set("spark.executor.memory", "2g")
spark.conf.set("spark.sql.shuffle.partitions", "50")
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")
```

### Production Environment  
```python
# Optimized for performance and reliability
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.executor.memoryOverhead", "1600m")
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "10000")
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

## 🔍 Common Issues & Solutions

| Issue | Symptoms | Solution |
|-------|----------|----------|
| **Data Skew** | Few tasks taking much longer | Enable `spark.sql.adaptive.skewJoin.enabled` |
| **Memory Pressure** | High GC time, OOM errors | Increase executor memory, reduce partition sizes |
| **Slow Shuffles** | High shuffle read/write times | Optimize partition count, use compression |
| **Streaming Lag** | Processing time > batch interval | Enable backpressure, adjust input rates |

## 📚 Additional Resources

### Official Spark Documentation
- [Spark SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Spark Streaming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Spark Configuration Reference](https://spark.apache.org/docs/latest/configuration.html)

### Advanced Reading
- [Catalyst Optimizer Deep Dive](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)
- [Adaptive Query Execution](https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html)
- [Whole-Stage Code Generation](https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html)

## 🤝 Contributing

This is a personal learning repository. If you find any issues or have suggestions for improvement:

1. Review the existing documentation thoroughly
2. Test any configuration changes in a development environment first
3. Document your findings and share your learnings
4. Consider the impact on different environments (dev/test/prod)

## 📊 Repository Structure

```
Pyspark/
├── HowToReadSparkQueryPlans.md        # Complete query plan analysis guide
├── spark_configuration_guide.md       # Comprehensive configuration reference  
├── HowToReadSparkDAG.md               # DAG analysis guide (future content)
└── resources/
    └── spark_query_plan_flow.png      # Query execution flow diagram
```

---

## 🎯 Next Steps

1. **Start with Query Plans**: Read the [Complete Query Plans Guide](./HowToReadSparkQueryPlans.md)
2. **Apply Configurations**: Use the [Configuration Guide](./spark_configuration_guide.md) for your environment
3. **Practice**: Apply learnings to real datasets and monitor performance improvements
4. **Iterate**: Use the performance monitoring guidelines to continuously optimize

---

*Happy Sparking! 🔥*

> 💡 **Pro Tip**: Always start with understanding your query execution plans before making configuration changes. The best optimizations come from understanding what Spark is actually doing with your data.
