# MovieLens Pipeline with HDFS Backend

## 🎯 Overview

This project demonstrates a complete Big Data pipeline using **HDFS (Hadoop Distributed File System)** as the storage backend, replacing MySQL/SQLite.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Data Ingestion Layer                      │
│  MovieLens Dataset → Download → Extract → Upload to HDFS   │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                      HDFS Storage                           │
│  /movielens/data/        - Raw ingested data               │
│  /movielens/output/      - Spark processed results         │
│  /movielens/reports/     - Generated reports               │
│  /movielens/checkpoints/ - ETL checkpoints                 │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│               Processing Layer (Pandas + PySpark)           │
│  • Pandas ETL Operations                                   │
│  • PySpark Analytics                                       │
│  • Collaborative Filtering                                 │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                    Reporting Layer                          │
│  • Top Movies Analysis                                     │
│  • User Segmentation                                       │
│  • Performance Metrics                                     │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker Desktop (running)
- Python 3.8+
- 8GB+ RAM recommended

### Step 1: Start HDFS Cluster

```powershell
# Navigate to project directory
cd c:\Users\ariha\Downloads\MovieLensPipeline

# Start Hadoop cluster with Docker Compose
docker-compose up -d

# Verify all containers are running
docker ps
```

You should see 5 containers running:
- `namenode` - HDFS NameNode
- `datanode` - HDFS DataNode
- `resourcemanager` - YARN ResourceManager
- `nodemanager` - YARN NodeManager
- `historyserver` - MapReduce History Server

### Step 2: Verify HDFS

```powershell
# Check HDFS status
docker exec namenode hdfs dfs -ls /

# Create required directories (already done in setup)
docker exec namenode hdfs dfs -ls /movielens
```

Expected output:
```
/movielens/data
/movielens/output
/movielens/reports
/movielens/checkpoints
```

### Step 3: Install Python Dependencies

```powershell
pip install -r requirements.txt
```

### Step 4: Run the Pipeline

```powershell
python main.py
```

The pipeline will:
1. ✅ Download MovieLens 100k dataset
2. ✅ Upload data to HDFS
3. ✅ Perform Pandas-based ETL operations
4. ✅ Execute PySpark analytics jobs
5. ✅ Generate comprehensive reports
6. ✅ Store all results in HDFS

## 📊 Configuration

Edit `main.py` to switch between HDFS and local storage:

```python
# HDFS Configuration
USE_HDFS = True   # Set to False for local filesystem
HDFS_URL = 'hdfs://localhost:9000'
```

## 🔍 Accessing Data

### View HDFS Web UI

Open browser: http://localhost:9870

- Browse files in HDFS
- Check cluster health
- Monitor storage usage

### Download Results from HDFS

```powershell
# List files
docker exec namenode hdfs dfs -ls /movielens/reports

# Download specific file
docker exec namenode hdfs dfs -get /movielens/reports/top_genres.csv /tmp/
docker cp namenode:/tmp/top_genres.csv .
```

### Read Files from HDFS (Python)

```python
import subprocess
from io import StringIO
import pandas as pd

result = subprocess.run([
    'docker', 'exec', 'namenode',
    'hdfs', 'dfs', '-cat', '/movielens/data/ratings.csv'
], capture_output=True, check=True)

df = pd.read_csv(StringIO(result.stdout.decode('utf-8')))
```

## 📁 HDFS Directory Structure

```
/movielens/
├── data/                    # Raw ingested data
│   ├── ratings.csv
│   ├── users.csv
│   └── items.csv
├── output/                  # Spark analytics results
│   ├── top_movies_per_genre.csv
│   └── user_segment_summary.csv
├── reports/                 # Generated reports
│   ├── top_genres.csv
│   ├── active_users.csv
│   ├── top_movies.csv
│   ├── performance_comparison.csv
│   └── summary_statistics.json
└── checkpoints/             # ETL checkpoints
    └── etl_results_*.csv
```

## 🛠️ Key Features

### 1. No MySQL/SQLite Dependencies
- ✅ All data stored in HDFS
- ✅ Distributed file system for scalability
- ✅ Fault-tolerant data storage

### 2. Seamless Integration
- Data ingestion automatically uploads to HDFS
- Pandas reads directly from HDFS
- PySpark uses HDFS natively
- Reports saved back to HDFS

### 3. Production-Ready
- Hadoop ecosystem (NameNode, DataNode, YARN)
- Scalable architecture
- Industry-standard big data stack

## 🔧 Useful Commands

### HDFS Operations

```powershell
# List directory contents
docker exec namenode hdfs dfs -ls /movielens/data

# Create directory
docker exec namenode hdfs dfs -mkdir -p /movielens/new_folder

# Upload file to HDFS
docker cp local_file.csv namenode:/tmp/
docker exec namenode hdfs dfs -put /tmp/local_file.csv /movielens/data/

# Download file from HDFS
docker exec namenode hdfs dfs -get /movielens/data/ratings.csv /tmp/
docker cp namenode:/tmp/ratings.csv .

# Remove file/directory
docker exec namenode hdfs dfs -rm -r /movielens/old_folder

# Check file size
docker exec namenode hdfs dfs -du -h /movielens
```

### Docker Operations

```powershell
# Start cluster
docker-compose up -d

# Stop cluster
docker-compose down

# View logs
docker logs namenode
docker logs datanode

# Restart specific service
docker-compose restart namenode
```

## 🎯 Performance Comparison

The pipeline compares Pandas vs PySpark performance for the same operations:

| Operation | Pandas | PySpark | Notes |
|-----------|--------|---------|-------|
| Data Loading | Fast | Medium | Overhead for small data |
| Aggregations | Medium | Fast | Parallel processing |
| Joins | Medium | Fast | Distributed joins |
| Overall | Better for small | Better for large | Crossover ~1GB |

## 🐛 Troubleshooting

### HDFS Connection Issues

```powershell
# Check if containers are running
docker ps

# Restart HDFS cluster
docker-compose down
docker-compose up -d

# Check namenode logs
docker logs namenode
```

### Port Already in Use

If port 9000 or 9870 is already in use:

1. Edit `docker-compose.yml`
2. Change port mappings (e.g., `9001:9000`)
3. Update `HDFS_URL` in `main.py`

### Data Not Uploading to HDFS

```powershell
# Verify HDFS is accessible
docker exec namenode hdfs dfs -ls /

# Check permissions
docker exec namenode hdfs dfs -chmod -R 777 /movielens
```

## 📈 Next Steps

1. **Scale Up**: Add more DataNodes for distributed storage
2. **Spark Cluster**: Connect to standalone Spark cluster
3. **Monitoring**: Add Grafana for HDFS metrics
4. **Security**: Enable Kerberos authentication
5. **Compression**: Use Parquet/ORC for better performance

## 🎓 Learning Resources

- [HDFS Architecture](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [PySpark with HDFS](https://spark.apache.org/docs/latest/api/python/)
- [Docker Hadoop Setup](https://github.com/big-data-europe/docker-hadoop)

## ✨ Benefits of HDFS Backend

1. **Scalability**: Handles petabytes of data
2. **Fault Tolerance**: Automatic replication
3. **Performance**: Parallel I/O operations
4. **Ecosystem**: Integrates with Spark, Hive, HBase
5. **Production-Ready**: Industry-standard for big data

---

## 🎉 Success!

Your MovieLens pipeline is now running with HDFS as the complete storage backend, replacing all MySQL/SQLite dependencies!

Access HDFS Web UI: http://localhost:9870
