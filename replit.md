# MovieLens 100k Big Data Pipeline

## Overview

This project is a demonstration big data pipeline that processes the MovieLens 100k dataset. It showcases an end-to-end data engineering workflow including data ingestion, ETL processing with Pandas, distributed analytics with PySpark, and report generation. The pipeline downloads movie ratings data from GroupLens, processes it through multiple stages, and generates analytical insights about movie preferences, user behavior, and genre popularity.

The system is designed as an educational example of modern data processing techniques, combining traditional data analysis tools (Pandas, SQLite) with distributed computing frameworks (Apache Spark) to handle and analyze movie rating data.

## Recent Changes

**October 4, 2025**: Completed full implementation of the Big Data MovieLens pipeline
- Implemented all core modules: ingest.py, process.py, spark_jobs.py, report.py, main.py
- Successfully tested end-to-end pipeline with 100,000 ratings from 943 users on 1,682 movies
- Generated comprehensive analytics including top movies per genre, user segmentation, and collaborative filtering (RMSE: 0.9197)
- Created performance comparison reports showing Pandas vs PySpark trade-offs for small datasets
- All data processing, analytics, and reporting features are functional and verified

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Data Pipeline Architecture

The system implements a classic ETL (Extract, Transform, Load) pipeline with the following stages:

1. **Ingestion Layer** (`ingest.py`): Downloads and extracts the MovieLens 100k dataset from grouplens.org, then loads it into Pandas DataFrames before storing as CSV files to simulate HDFS storage
2. **Processing Layer** (`process.py`): Performs Pandas-based ETL transformations and manages SQLite database operations for structured storage
3. **Analytics Layer** (`spark_jobs.py`): Executes PySpark jobs for distributed analytics including genre analysis, user segmentation, and collaborative filtering using Spark ML
4. **Reporting Layer** (`report.py`): Generates summary reports and exports analytics results in CSV and JSON formats
5. **Orchestration** (`main.py`): Coordinates the execution of all pipeline stages in sequence

### Data Flow

Data flows through the system in a linear pipeline:
- Raw data → CSV storage (simulating HDFS) → Pandas processing → SQLite database
- CSV storage → PySpark DataFrames → Analytics results → Report files

This design allows for both traditional single-node processing (Pandas) and distributed processing (Spark) on the same dataset, demonstrating different approaches to data analysis.

### Storage Strategy

The system uses a multi-tiered storage approach:

1. **Raw Data**: Downloaded ZIP files stored in `data/raw/`
2. **Processed Data**: CSV files in `data/processed/` simulating HDFS distributed storage
3. **Structured Storage**: SQLite database at `data/movielens.db` for relational queries
4. **Analytics Output**: Spark results in `data/spark_output/`
5. **Reports**: Final reports in `data/reports/` (CSV and JSON formats)

This hybrid approach was chosen to demonstrate different storage paradigms used in big data systems while keeping infrastructure requirements minimal.

### Processing Frameworks

**Pandas Processing**: Used for initial ETL, data cleaning, and lightweight analytics. Chosen for its simplicity and efficiency with datasets that fit in memory (~100k records).

**PySpark Processing**: Employed for demonstration of distributed computing patterns including:
- Collaborative filtering with ALS (Alternating Least Squares) algorithm
- Genre-based analytics with DataFrame operations
- User segmentation analysis

The dual-framework approach shows how to transition from single-node to distributed processing as data scales.

### Machine Learning Components

The system includes a basic collaborative filtering recommendation system using Spark MLlib's ALS algorithm. This demonstrates integration of machine learning into data pipelines for generating movie recommendations based on user rating patterns.

## External Dependencies

### Third-Party Libraries

- **pandas**: Core data manipulation and analysis
- **pyspark**: Distributed data processing and machine learning (Spark SQL, Spark ML)
- **requests**: HTTP client for downloading datasets from grouplens.org
- **sqlite3**: Built-in Python library for relational database operations

### External Data Sources

- **GroupLens MovieLens 100k Dataset**: Downloaded from `https://files.grouplens.org/datasets/movielens/ml-100k.zip`
  - Contains 100,000 movie ratings from 943 users on 1,682 movies
  - Includes user demographic data (age, gender, occupation)
  - Requires acknowledgment in publications per GroupLens usage license

### Database Systems

- **SQLite**: Embedded relational database used for storing and querying user data
  - Chosen for zero-configuration setup and portability
  - Stores processed user demographic information
  - Enables SQL-based analysis alongside NoSQL-style CSV processing

### Apache Spark Configuration

- **Local Mode**: Spark runs in local mode with `master("local[*]")` to use all available cores
- **Memory Allocation**: Driver memory set to 2GB
- **Shuffle Partitions**: Configured to 4 partitions for optimal performance on small datasets
- **Log Level**: Set to WARN to reduce verbose output

The Spark configuration is optimized for single-machine demonstration rather than production cluster deployment.