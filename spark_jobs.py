"""
PySpark Analytics Module

This module implements Spark-based analytics jobs including:
- Top movies per genre analysis
- User segmentation by rating habits
- Simple collaborative filtering using ML
"""

import time
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, explode, array, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator


class SparkAnalytics:
    """Handles PySpark-based analytics operations with HDFS support."""
    
    def __init__(self, processed_dir='data/processed', output_dir='data/spark_output',
                 use_hdfs=False, hdfs_url='hdfs://localhost:9000'):
        """
        Initialize Spark analytics.
        
        Args:
            processed_dir: Directory containing processed CSV files
            output_dir: Directory for Spark output files
            use_hdfs: Whether to use HDFS for storage
            hdfs_url: HDFS namenode URL
        """
        self.use_hdfs = use_hdfs
        self.hdfs_url = hdfs_url
        
        if use_hdfs:
            self.processed_dir = f"{hdfs_url}/movielens/data"
            self.output_dir = f"{hdfs_url}/movielens/output"
        else:
            self.processed_dir = Path(processed_dir)
            self.output_dir = Path(output_dir)
            self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Spark session
        print("Initializing Spark session...")
        spark_builder = SparkSession.builder \
            .appName("MovieLens Analytics") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "4")
        
        # Configure HDFS if enabled
        if use_hdfs:
            spark_builder = spark_builder \
                .config("spark.hadoop.fs.defaultFS", hdfs_url)
            print(f"Spark configured for HDFS: {hdfs_url}")
        
        self.spark = spark_builder.getOrCreate()
        
        # Set log level to reduce verbosity
        self.spark.sparkContext.setLogLevel("WARN")
        print("Spark session initialized")
    
    def load_data(self):
        """
        Load data from CSV files into Spark DataFrames (HDFS or local).
        
        Returns:
            Tuple of (ratings_df, users_df, items_df)
        """
        print("\nLoading data into Spark DataFrames...")
        
        if self.use_hdfs:
            # Load from HDFS
            ratings_path = f"{self.processed_dir}/ratings.csv"
            users_path = f"{self.processed_dir}/users.csv"
            items_path = f"{self.processed_dir}/items.csv"
        else:
            # Load from local filesystem
            ratings_path = str(self.processed_dir / 'ratings.csv')
            users_path = str(self.processed_dir / 'users.csv')
            items_path = str(self.processed_dir / 'items.csv')
        
        # Load ratings
        ratings_df = self.spark.read.csv(
            ratings_path,
            header=True,
            inferSchema=True
        )
        
        # Load users
        users_df = self.spark.read.csv(
            users_path,
            header=True,
            inferSchema=True
        )
        
        # Load items
        items_df = self.spark.read.csv(
            items_path,
            header=True,
            inferSchema=True
        )
        
        print(f"Loaded {ratings_df.count()} ratings, {users_df.count()} users, {items_df.count()} items")
        return ratings_df, users_df, items_df
    
    def top_movies_per_genre(self, ratings_df, items_df, top_n=10):
        """
        Find top N movies for each genre based on average rating.
        
        Args:
            ratings_df: Spark DataFrame with ratings
            items_df: Spark DataFrame with items
            top_n: Number of top movies to return per genre
            
        Returns:
            Spark DataFrame with top movies per genre
        """
        print(f"\nAnalyzing top {top_n} movies per genre...")
        
        # Calculate average rating for each movie
        movie_ratings = ratings_df.groupBy('item_id') \
            .agg(avg('rating').alias('avg_rating'),
                 count('rating').alias('num_ratings')) \
            .filter(col('num_ratings') >= 10)  # Filter movies with at least 10 ratings
        
        # Join with items to get genres
        movies_with_ratings = items_df.join(movie_ratings, 'item_id')
        
        # Define genre columns
        genre_columns = ['Action', 'Adventure', 'Animation', 'Children', 'Comedy',
                        'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir',
                        'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi',
                        'Thriller', 'War', 'Western']
        
        # Analyze top movies for each genre
        top_movies_list = []
        
        for genre in genre_columns:
            if genre in items_df.columns:
                genre_movies = movies_with_ratings \
                    .filter(col(genre) == 1) \
                    .select('item_id', 'title', 'avg_rating', 'num_ratings') \
                    .orderBy(desc('avg_rating')) \
                    .limit(top_n)
                
                # Add genre column
                genre_movies = genre_movies.withColumn('genre', lit(genre))
                top_movies_list.append(genre_movies)
        
        # Union all genre results
        if top_movies_list:
            top_movies_df = top_movies_list[0]
            for df in top_movies_list[1:]:
                top_movies_df = top_movies_df.union(df)
            
            print(f"Found top movies for {len(genre_columns)} genres")
            return top_movies_df
        
        return None
    
    def user_segmentation(self, ratings_df, users_df):
        """
        Segment users based on their rating habits.
        
        Args:
            ratings_df: Spark DataFrame with ratings
            users_df: Spark DataFrame with users
            
        Returns:
            Spark DataFrame with user segments
        """
        print("\nSegmenting users by rating habits...")
        
        # Calculate user rating statistics
        user_stats = ratings_df.groupBy('user_id') \
            .agg(
                count('rating').alias('num_ratings'),
                avg('rating').alias('avg_rating')
            )
        
        # Join with user demographics
        user_segments = user_stats.join(users_df, 'user_id')
        
        # Create segments based on rating behavior
        # Segment 1: Rating frequency (low, medium, high)
        # Segment 2: Rating tendency (harsh, neutral, generous)
        user_segments = user_segments.withColumn(
            'frequency_segment',
            when(col('num_ratings') < 50, 'low')
            .when(col('num_ratings') < 150, 'medium')
            .otherwise('high')
        )
        
        user_segments = user_segments.withColumn(
            'rating_tendency',
            when(col('avg_rating') < 3.0, 'harsh')
            .when(col('avg_rating') < 4.0, 'neutral')
            .otherwise('generous')
        )
        
        # Segment summary
        segment_summary = user_segments.groupBy('frequency_segment', 'rating_tendency') \
            .agg(count('user_id').alias('user_count')) \
            .orderBy('frequency_segment', 'rating_tendency')
        
        print("User segmentation completed")
        return user_segments, segment_summary
    
    def collaborative_filtering(self, ratings_df, max_iter=10, reg_param=0.1):
        """
        Simple collaborative filtering using ALS (Alternating Least Squares).
        
        Args:
            ratings_df: Spark DataFrame with ratings
            max_iter: Maximum iterations for ALS
            reg_param: Regularization parameter
            
        Returns:
            Tuple of (model, predictions, rmse)
        """
        print("\nTraining collaborative filtering model (ALS)...")
        
        # Split data into training and test sets
        train_data, test_data = ratings_df.randomSplit([0.8, 0.2], seed=42)
        
        print(f"Training set: {train_data.count()} ratings")
        print(f"Test set: {test_data.count()} ratings")
        
        # Build ALS model
        als = ALS(
            maxIter=max_iter,
            regParam=reg_param,
            userCol="user_id",
            itemCol="item_id",
            ratingCol="rating",
            coldStartStrategy="drop",
            seed=42
        )
        
        # Train model
        start_time = time.time()
        model = als.fit(train_data)
        training_time = time.time() - start_time
        
        print(f"Model training completed in {training_time:.2f} seconds")
        
        # Make predictions
        predictions = model.transform(test_data)
        
        # Evaluate model
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )
        rmse = evaluator.evaluate(predictions)
        
        print(f"Model RMSE: {rmse:.4f}")
        
        return model, predictions, rmse
    
    def generate_recommendations(self, model, user_id, num_recommendations=10):
        """
        Generate movie recommendations for a specific user.
        
        Args:
            model: Trained ALS model
            user_id: User ID to generate recommendations for
            num_recommendations: Number of recommendations
            
        Returns:
            Spark DataFrame with recommendations
        """
        # Generate top N recommendations for the user
        user_recs = model.recommendForAllUsers(num_recommendations)
        
        # Filter for specific user
        user_rec = user_recs.filter(col('user_id') == user_id)
        
        return user_rec
    
    def save_results(self, df, filename):
        """
        Save Spark DataFrame to CSV (HDFS or local).
        
        Args:
            df: Spark DataFrame to save
            filename: Output filename
        """
        if self.use_hdfs:
            # Save to HDFS directly using Spark
            output_path = f"{self.output_dir}/{filename}"
            
            # Save to a temporary directory in HDFS
            temp_path = f"{output_path}_temp"
            df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(temp_path)
            
            print(f"Saved to HDFS: {output_path}")
            print(f"  (Spark output in: {temp_path})")
        else:
            # Save to local filesystem
            output_path = self.output_dir / filename
            
            # Convert to Pandas and save (for small datasets)
            pandas_df = df.toPandas()
            pandas_df.to_csv(output_path, index=False)
            print(f"Saved {len(pandas_df)} records to {output_path}")
    
    def run_spark_analytics(self):
        """
        Execute all Spark analytics jobs.
        
        Returns:
            Dictionary containing all analytics results
        """
        print("=" * 60)
        print("Starting PySpark Analytics Jobs")
        if self.use_hdfs:
            print(f"Storage Backend: HDFS ({self.hdfs_url})")
        else:
            print("Storage Backend: Local Filesystem")
        print("=" * 60)
        
        start_time = time.time()
        
        # Load data
        ratings_df, users_df, items_df = self.load_data()
        
        # Job 1: Top movies per genre
        top_movies_df = self.top_movies_per_genre(ratings_df, items_df)
        if top_movies_df:
            self.save_results(top_movies_df, 'top_movies_per_genre.csv')
        
        # Job 2: User segmentation
        user_segments_df, segment_summary_df = self.user_segmentation(ratings_df, users_df)
        self.save_results(segment_summary_df, 'user_segment_summary.csv')
        
        # Job 3: Collaborative filtering
        model, predictions, rmse = self.collaborative_filtering(ratings_df)
        
        # Generate sample recommendations for user 1
        sample_recs = self.generate_recommendations(model, user_id=1, num_recommendations=10)
        if sample_recs.count() > 0:
            print("\nSample recommendations for user 1:")
            sample_recs.show(truncate=False)
        
        elapsed = time.time() - start_time
        
        print("\n" + "=" * 60)
        print(f"PySpark Analytics Completed in {elapsed:.2f} seconds")
        print("=" * 60)
        
        return {
            'top_movies': top_movies_df,
            'user_segments': user_segments_df,
            'segment_summary': segment_summary_df,
            'cf_model': model,
            'cf_rmse': rmse,
            'processing_time': elapsed
        }
    
    def stop_spark(self):
        """Stop the Spark session."""
        print("\nStopping Spark session...")
        self.spark.stop()
        print("Spark session stopped")


# Import when function for conditional column creation
from pyspark.sql.functions import when


if __name__ == "__main__":
    # Test the Spark analytics module
    analytics = SparkAnalytics()
    
    try:
        results = analytics.run_spark_analytics()
        
        # Display some results
        if results['top_movies']:
            print("\nSample Top Movies:")
            results['top_movies'].show(10, truncate=False)
        
        print("\nUser Segment Summary:")
        results['segment_summary'].show()
        
        print(f"\nCollaborative Filtering RMSE: {results['cf_rmse']:.4f}")
        
    finally:
        analytics.stop_spark()
