# MovieLens 100k Big Data Pipeline

A comprehensive demonstration of an end-to-end big data pipeline processing the MovieLens 100k dataset. This project showcases modern data engineering practices using Pandas, PySpark, SQLite, and interactive Streamlit visualizations.

##  Overview

This pipeline demonstrates:
- **Data Ingestion**: Automated download and loading from GroupLens Research
- **ETL Processing**: Pandas-based transformations with SQLite integration
- **Distributed Analytics**: PySpark jobs for collaborative filtering and segmentation
- **Interactive Dashboard**: Streamlit web application with comprehensive visualizations
- **Performance Benchmarking**: Pandas vs PySpark comparison

### Dataset
- **Source**: [GroupLens MovieLens 100k](https://grouplens.org/datasets/movielens/100k/)
- **Size**: 100,000 ratings from 943 users on 1,682 movies
- **Features**: User demographics, movie genres, timestamps

##  Requirements

### System Requirements
- **Python**: 3.8 or higher
- **Java**: JDK 11 or higher (required for PySpark)
- **RAM**: Minimum 4GB recommended
- **Disk Space**: ~50MB for dataset and outputs

### Python Dependencies
```
pandas>=2.0.0
pyspark>=3.5.0
requests>=2.31.0
streamlit>=1.30.0
plotly>=5.18.0
```

##  Installation & Setup

### 1. Clone or Download the Project

```bash
# If using git
git clone https://github.com/arihantjain6739/MovieLensPipeline
cd movielens-pipeline

# Or download and extract the ZIP file
```

### 2. Install Python Dependencies

**Using pip:**
```bash
pip install pandas pyspark requests streamlit plotly
```

**Using a virtual environment (recommended):**
```bash
# Create virtual environment
python -m venv venv

# Activate it
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install pandas pyspark requests streamlit plotly
```

**Using requirements.txt:**
```bash
pip install -r requirements.txt
```

### 3. Verify Java Installation (for PySpark)

```bash
java -version
```

If Java is not installed:
- **Windows**: Download from [Oracle](https://www.oracle.com/java/technologies/downloads/) or [OpenJDK](https://adoptium.net/)
- **macOS**: `brew install openjdk@11`
- **Linux**: `sudo apt install openjdk-11-jdk`

##  Running the Pipeline

### Step 1: Run the Data Pipeline

**First time or to refresh data:**
```bash
python main.py
```

This will:
1. Download MovieLens 100k dataset (~5MB)
2. Extract and load data into Pandas
3. Run ETL transformations with SQLite
4. Execute PySpark analytics (collaborative filtering, segmentation)
5. Generate comprehensive reports

**Expected Output:**
- `data/processed/` - CSV files (simulating HDFS)
- `data/reports/` - Summary reports (CSV/JSON)
- `data/spark_output/` - PySpark results
- `data/movielens.db` - SQLite database

**Processing Time:** ~30-40 seconds on most machines

### Step 2: Launch the Dashboard

**Run the Streamlit dashboard:**
```bash
streamlit run app.py
```

The dashboard will automatically open in your browser at:
```
http://localhost:8501
```

**If port 8501 is already in use**, Streamlit will automatically try the next available port (8502, 8503, etc.). Check the terminal output for the actual URL.

**To specify a custom port:**
```bash
streamlit run app.py --server.port 8080
```

**For headless server deployment:**
```bash
streamlit run app.py --server.port 8501 --server.address 0.0.0.0 --server.headless true
```

##  Using the Dashboard

The dashboard has **5 interactive pages**:

### 1.  Overview
- Key metrics (ratings, users, movies)
- Dataset statistics
- Top 5 genres by popularity
- Top 10 highest-rated movies

### 2.  Dataset Explorer
- **Ratings Tab**: Distribution of ratings, ratings per user
- **Users Tab**: Age distribution, gender breakdown, occupation stats
- **Movies Tab**: Genre distribution, sample movie data

### 3.  Analytics Results
- **Genre Analytics**: Popularity rankings, quality vs quantity analysis
- **Movie Rankings**: Top movies with interactive filtering
- **User Activity**: Activity by occupation, rating behavior patterns

### 4.  Performance Comparison
- Pandas vs PySpark processing time comparison
- Framework trade-off analysis
- When to use each framework

### 5.  User Insights
- User segmentation by rating behavior
- Demographic analysis (age, gender)
- Rating tendency distribution

##  Project Structure

```
movielens-pipeline/
├── app.py                  # Streamlit dashboard (main UI)
├── main.py                 # Pipeline orchestrator
├── ingest.py               # Data ingestion module
├── process.py              # Pandas ETL & SQLite operations
├── spark_jobs.py           # PySpark analytics jobs
├── report.py               # Report generation
├── README.md               # This file
├── requirements.txt        # Python dependencies
└── data/                   # Generated data (not in git)
    ├── raw/                # Downloaded MovieLens data
    ├── processed/          # CSV files (HDFS simulation)
    ├── reports/            # Generated reports
    ├── spark_output/       # PySpark results
    ├── checkpoints/        # ETL checkpoints
    └── movielens.db        # SQLite database
```

##  Troubleshooting

### Issue: "This site can't be reached" or ERR_ADDRESS_INVALID

**Problem**: Trying to access `http://0.0.0.0:8501/` in browser

**Solution**: Use `http://localhost:8501/` instead

- `0.0.0.0` is a bind address (server listens on all interfaces)
- You must access via `localhost` or `127.0.0.1`

### Issue: Port already in use

**Problem**: `OSError: [Errno 98] Address already in use`

**Solutions**:
1. Streamlit will auto-increment to next port (check terminal output)
2. Manually specify a different port:
   ```bash
   streamlit run app.py --server.port 8080
   ```
3. Kill the process using the port:
   ```bash
   # Find process
   lsof -i :8501  # macOS/Linux
   netstat -ano | findstr :8501  # Windows
   
   # Kill it
   kill -9 <PID>  # macOS/Linux
   taskkill /PID <PID> /F  # Windows
   ```

### Issue: "Data files not found" error in dashboard

**Problem**: Dashboard can't find analytics data

**Solution**: Run the pipeline first:
```bash
python main.py
```

### Issue: Java not found (PySpark error)

**Problem**: `JAVA_HOME is not set` or Java gateway error

**Solution**:
1. Install Java JDK 11+
2. Set JAVA_HOME environment variable:
   ```bash
   # macOS/Linux
   export JAVA_HOME=$(/usr/libexec/java_home)  # macOS
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  # Linux
   
   # Windows
   set JAVA_HOME=C:\Program Files\Java\jdk-11
   ```

### Issue: "Figure object has no attribute 'update_xaxis'"

**Problem**: Plotly version compatibility

**Solution**: This has been fixed. Update to latest version:
```bash
pip install --upgrade plotly
```

### Issue: Slow PySpark initialization

**Expected Behavior**: PySpark takes 5-10 seconds to initialize (Java gateway startup)

If it takes longer:
- Ensure you have at least 2GB free RAM
- Close other Java applications
- PySpark overhead is normal for small datasets

##  Key Insights from the Pipeline

### Dataset Statistics
- **Total Ratings**: 100,000
- **Sparsity**: ~93.7% (most user-movie pairs unrated)
- **Average Rating**: ~3.53 / 5.0
- **Most Popular Genre**: Drama (39,895 ratings)

### Performance Results
- **Pandas Processing**: ~0.3 seconds
- **PySpark Processing**: ~20-25 seconds
- **Winner for 100k dataset**: Pandas (80x faster)

**Why?** PySpark has significant overhead (JVM startup, task scheduling) that dominates for small datasets. PySpark excels with datasets >10GB across distributed clusters.

### Machine Learning
- **Collaborative Filtering**: ALS algorithm
- **RMSE**: ~0.92 on test set
- **Use Case**: Movie recommendations based on user rating patterns

##  Analytics Features

### Pandas ETL
- Active user filtering (min 50 ratings)
- Movie statistics (avg rating, count)
- User preference analysis by demographics
- Genre popularity rankings
- SQL joins with external demographics

### PySpark Jobs
- Top movies per genre
- User segmentation (frequency × tendency)
- Collaborative filtering (ALS)
- Distributed aggregations
- Performance benchmarking

### Reports Generated
1. `top_genres.csv` - Genre popularity rankings
2. `active_users.csv` - Most active users
3. `top_movies.csv` - Highest-rated movies
4. `performance_comparison.csv` - Pandas vs Spark
5. `summary_statistics.json` - Dataset overview

##  Deployment Options

### Local Development
```bash
streamlit run app.py
```

### Network Access (LAN)
```bash
streamlit run app.py --server.address 0.0.0.0
```
Access via: `http://<your-ip>:8501`

### Cloud Deployment (Streamlit Community Cloud)
1. Push to GitHub
2. Visit [share.streamlit.io](https://share.streamlit.io)
3. Connect repository
4. Deploy!

### Docker (Optional)
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN apt-get update && apt-get install -y openjdk-11-jdk
RUN pip install -r requirements.txt
COPY . .
CMD ["streamlit", "run", "app.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
```

##  Notes

- **Data Persistence**: The `data/` directory is generated by the pipeline
- **Rerun Pipeline**: Delete `data/` and run `python main.py` to start fresh
- **Checkpoints**: ETL checkpoints enable fault tolerance
- **SQLite**: Lightweight database for demo; use PostgreSQL for production

##  Acknowledgments

- **Dataset**: GroupLens Research, University of Minnesota
- **Citation**: F. Maxwell Harper and Joseph A. Konstan. 2015. The MovieLens Datasets: History and Context. ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4: 19:1–19:19.

##  License

This project is for educational purposes. The MovieLens dataset has its own usage license from GroupLens Research.

##  Support

If you encounter any issues:
1. Check the Troubleshooting section above
2. Ensure all dependencies are installed
3. Verify Java is installed for PySpark
4. Run `python main.py` before launching dashboard
5. Use `http://localhost:8501/` (not `http://0.0.0.0:8501/`)

---

**Happy Data Engineering! 🚀**
