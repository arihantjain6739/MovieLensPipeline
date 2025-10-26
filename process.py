"""
Data Processing Module with Pandas ETL and HDFS Integration

This module handles data processing operations including ETL transformations,
aggregations, and HDFS data operations for the MovieLens dataset.
"""

import pandas as pd
from pathlib import Path
import time
import subprocess
import tempfile
import os


class DataProcessor:
    """Handles pandas-based ETL with HDFS support."""
    
    def __init__(self, processed_dir='data/processed', checkpoint_dir='data/checkpoints',
                 use_hdfs=False, hdfs_url='hdfs://localhost:9000'):
        """
        Initialize the processor.
        
        Args:
            processed_dir: Directory containing processed CSV files
            checkpoint_dir: Directory for ETL checkpoints
            use_hdfs: Whether to use HDFS for storage
            hdfs_url: HDFS namenode URL
        """
        self.use_hdfs = use_hdfs
        self.hdfs_url = hdfs_url
        
        if use_hdfs:
            self.processed_dir = '/movielens/data'
            self.checkpoint_dir = '/movielens/checkpoints'
            print(f"DataProcessor using HDFS: {hdfs_url}")
        else:
            self.processed_dir = Path(processed_dir)
            self.checkpoint_dir = Path(checkpoint_dir)
            self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
            print(f"DataProcessor using local filesystem")
    
    def load_data(self):
        """
        Load processed data from CSV files (HDFS or local).
        
        Returns:
            Tuple of (ratings_df, users_df, items_df)
        """
        print("Loading processed data from storage...")
        
        if self.use_hdfs:
            # Load from HDFS
            ratings_df = self._read_from_hdfs('ratings.csv')
            users_df = self._read_from_hdfs('users.csv')
            items_df = self._read_from_hdfs('items.csv')
        else:
            # Load from local filesystem
            ratings_df = pd.read_csv(self.processed_dir / 'ratings.csv')
            users_df = pd.read_csv(self.processed_dir / 'users.csv')
            items_df = pd.read_csv(self.processed_dir / 'items.csv')
        
        print(f"Loaded {len(ratings_df)} ratings, {len(users_df)} users, {len(items_df)} items")
        return ratings_df, users_df, items_df
    
    def _read_from_hdfs(self, filename):
        """
        Read CSV file from HDFS.
        
        Args:
            filename: Name of the file to read
            
        Returns:
            pandas DataFrame
        """
        hdfs_path = f"{self.processed_dir}/{filename}"
        print(f"Reading from HDFS: {hdfs_path}")
        
        try:
            result = subprocess.run([
                'docker', 'exec', 'namenode',
                'hdfs', 'dfs', '-cat', hdfs_path
            ], capture_output=True, check=True)
            
            from io import StringIO
            return pd.read_csv(StringIO(result.stdout.decode('utf-8')))
        except subprocess.CalledProcessError as e:
            print(f"Error reading from HDFS: {e.stderr.decode()}")
            raise
    
    def _write_to_hdfs(self, df, filename):
        """
        Write DataFrame to HDFS.
        
        Args:
            df: pandas DataFrame to write
            filename: Name of the file
        """
        hdfs_path = f"{self.checkpoint_dir}/{filename}"
        print(f"Writing to HDFS: {hdfs_path}")
        
        # Write to temporary file first
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
            df.to_csv(tmp.name, index=False)
            tmp_path = tmp.name
        
        try:
            subprocess.run([
                'docker', 'exec', '-i', 'namenode',
                'hdfs', 'dfs', '-put', '-f', '-', hdfs_path
            ], stdin=open(tmp_path, 'rb'), check=True, capture_output=True)
            print(f"✓ Saved to HDFS: {hdfs_path}")
        except subprocess.CalledProcessError as e:
            print(f"Error writing to HDFS: {e.stderr.decode()}")
            raise
        finally:
            os.unlink(tmp_path)
    
    def filter_active_users(self, ratings_df, min_ratings=50):
        """
        Filter users who have rated at least min_ratings movies.
        
        Args:
            ratings_df: Ratings DataFrame
            min_ratings: Minimum number of ratings required
            
        Returns:
            DataFrame with active users
        """
        print(f"\nFiltering active users (min {min_ratings} ratings)...")
        
        user_counts = ratings_df.groupby('user_id').size()
        active_user_ids = user_counts[user_counts >= min_ratings].index
        
        active_ratings = ratings_df[ratings_df['user_id'].isin(active_user_ids)]
        print(f"Found {len(active_user_ids)} active users with {len(active_ratings)} ratings")
        
        return active_ratings, active_user_ids
    
    def calculate_movie_stats(self, ratings_df, items_df):
        """
        Calculate statistics for each movie (avg rating, count).
        
        Args:
            ratings_df: Ratings DataFrame
            items_df: Items DataFrame
            
        Returns:
            DataFrame with movie statistics
        """
        print("\nCalculating movie statistics...")
        
        movie_stats = ratings_df.groupby('item_id').agg({
            'rating': ['mean', 'count']
        }).reset_index()
        
        movie_stats.columns = ['item_id', 'avg_rating', 'rating_count']
        
        # Join with movie titles
        movie_stats = movie_stats.merge(
            items_df[['item_id', 'title']],
            on='item_id',
            how='left'
        )
        
        print(f"Calculated statistics for {len(movie_stats)} movies")
        return movie_stats
    
    def analyze_user_preferences(self, ratings_df, users_df):
        """
        Analyze user preferences by joining ratings with user demographics.
        
        Args:
            ratings_df: Ratings DataFrame
            users_df: Users DataFrame
            
        Returns:
            DataFrame with user preferences analysis
        """
        print("\nAnalyzing user preferences...")
        
        # Join ratings with users
        user_ratings = ratings_df.merge(users_df, on='user_id', how='left')
        
        # Calculate average rating by age group and gender
        user_ratings['age_group'] = pd.cut(
            user_ratings['age'],
            bins=[0, 18, 25, 35, 50, 100],
            labels=['<18', '18-25', '25-35', '35-50', '50+']
        )
        
        preferences = user_ratings.groupby(['age_group', 'gender']).agg({
            'rating': ['mean', 'count']
        }).reset_index()
        
        preferences.columns = ['age_group', 'gender', 'avg_rating', 'rating_count']
        
        print(f"Analyzed preferences for {len(preferences)} demographic segments")
        return preferences
    
    def get_genre_columns(self, items_df):
        """
        Get list of genre columns from items DataFrame.
        
        Args:
            items_df: Items DataFrame
            
        Returns:
            List of genre column names
        """
        genre_columns = ['Action', 'Adventure', 'Animation', 'Children', 'Comedy',
                        'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir',
                        'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi',
                        'Thriller', 'War', 'Western']
        return [col for col in genre_columns if col in items_df.columns]
    
    def analyze_genre_popularity(self, ratings_df, items_df):
        """
        Analyze which genres are most popular based on ratings.
        
        Args:
            ratings_df: Ratings DataFrame
            items_df: Items DataFrame
            
        Returns:
            DataFrame with genre popularity statistics
        """
        print("\nAnalyzing genre popularity...")
        
        # Join ratings with items
        ratings_items = ratings_df.merge(items_df, on='item_id', how='left')
        
        # Get genre columns
        genre_columns = self.get_genre_columns(items_df)
        
        # Calculate statistics for each genre
        genre_stats = []
        for genre in genre_columns:
            genre_movies = ratings_items[ratings_items[genre] == 1]
            if len(genre_movies) > 0:
                genre_stats.append({
                    'genre': genre,
                    'avg_rating': genre_movies['rating'].mean(),
                    'num_ratings': len(genre_movies),
                    'num_movies': items_df[items_df[genre] == 1].shape[0]
                })
        
        genre_df = pd.DataFrame(genre_stats).sort_values('num_ratings', ascending=False)
        print(f"Analyzed {len(genre_df)} genres")
        return genre_df
    
    def create_checkpoint(self, data_dict, checkpoint_name):
        """
        Create checkpoint for fault tolerance (stored in HDFS or local).
        
        Args:
            data_dict: Dictionary of DataFrames to checkpoint
            checkpoint_name: Name of the checkpoint
        """
        print(f"\nCreating checkpoint: {checkpoint_name}...")
        
        for name, df in data_dict.items():
            filename = f"{checkpoint_name}_{name}.csv"
            
            if self.use_hdfs:
                self._write_to_hdfs(df, filename)
            else:
                filepath = self.checkpoint_dir / filename
                df.to_csv(filepath, index=False)
                print(f"✓ Saved checkpoint to {filepath}")
        
        print(f"Checkpoint '{checkpoint_name}' created successfully")
    
    def run_etl_pipeline(self):
        """
        Execute the complete ETL pipeline with pandas.
        
        Returns:
            Dictionary containing all processed data
        """
        print("=" * 60)
        print("Starting Pandas ETL Pipeline")
        if self.use_hdfs:
            print(f"Storage Backend: HDFS ({self.hdfs_url})")
        else:
            print("Storage Backend: Local Filesystem")
        print("=" * 60)
        
        start_time = time.time()
        
        # Load data from storage
        ratings_df, users_df, items_df = self.load_data()
        
        # ETL operations
        active_ratings, active_user_ids = self.filter_active_users(ratings_df)
        movie_stats = self.calculate_movie_stats(ratings_df, items_df)
        user_preferences = self.analyze_user_preferences(ratings_df, users_df)
        genre_popularity = self.analyze_genre_popularity(ratings_df, items_df)
        
        # Import external data and perform SQL join
        demographics_df = self.import_external_demographics()
        joined_df = self.sql_join_demo()
        
        # Create checkpoint
        self.create_checkpoint({
            'movie_stats': movie_stats,
            'user_preferences': user_preferences,
            'genre_popularity': genre_popularity
        }, 'etl_results')
        
        elapsed = time.time() - start_time
        
        print("\n" + "=" * 60)
        print(f"Pandas ETL Pipeline Completed in {elapsed:.2f} seconds")
        print("=" * 60)
        
        return {
            'ratings': ratings_df,
            'users': users_df,
            'items': items_df,
            'active_ratings': active_ratings,
            'movie_stats': movie_stats,
            'user_preferences': user_preferences,
            'genre_popularity': genre_popularity,
            'demographics': demographics_df,
            'joined_data': joined_df,
            'processing_time': elapsed
        }


if __name__ == "__main__":
    # Test the processing module
    processor = DataProcessor()
    results = processor.run_etl_pipeline()
    
    # Display results
    print("\nTop 10 Movies by Average Rating:")
    top_movies = results['movie_stats'].sort_values('avg_rating', ascending=False).head(10)
    print(top_movies)
    
    print("\nGenre Popularity:")
    print(results['genre_popularity'].head(10))
