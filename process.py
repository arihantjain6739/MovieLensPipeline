"""
Data Processing Module with Pandas ETL and SQLite Integration

This module handles data processing operations including ETL transformations,
aggregations, and SQLite database operations for the MovieLens dataset.
"""

import pandas as pd
import sqlite3
from pathlib import Path
import time


class DataProcessor:
    """Handles pandas-based ETL and SQLite operations."""
    
    def __init__(self, processed_dir='data/processed', db_path='data/movielens.db'):
        """
        Initialize the processor.
        
        Args:
            processed_dir: Directory containing processed CSV files
            db_path: Path to SQLite database
        """
        self.processed_dir = Path(processed_dir)
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    def load_data(self):
        """
        Load processed data from CSV files.
        
        Returns:
            Tuple of (ratings_df, users_df, items_df)
        """
        print("Loading processed data from CSV files...")
        
        ratings_df = pd.read_csv(self.processed_dir / 'ratings.csv')
        users_df = pd.read_csv(self.processed_dir / 'users.csv')
        items_df = pd.read_csv(self.processed_dir / 'items.csv')
        
        print(f"Loaded {len(ratings_df)} ratings, {len(users_df)} users, {len(items_df)} items")
        return ratings_df, users_df, items_df
    
    def create_sqlite_db(self, users_df):
        """
        Create SQLite database and import users table.
        
        Args:
            users_df: Users DataFrame to import
        """
        print(f"\nCreating SQLite database at {self.db_path}...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Import users table
        users_df.to_sql('users', conn, if_exists='replace', index=False)
        print(f"Imported {len(users_df)} users into SQLite database")
        
        # Create index for better query performance
        cursor = conn.cursor()
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON users(user_id)')
        conn.commit()
        
        conn.close()
        print("SQLite database created successfully")
    
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
    
    def import_external_demographics(self):
        """
        Import external demographics CSV into SQLite for integration demo.
        
        Returns:
            DataFrame with demographics data
        """
        print("\nImporting external demographics data...")
        
        demographics_path = self.processed_dir / 'demographics.csv'
        
        if not demographics_path.exists():
            print(f"Demographics file not found at {demographics_path}")
            return None
        
        demographics_df = pd.read_csv(demographics_path)
        
        conn = sqlite3.connect(self.db_path)
        demographics_df.to_sql('demographics', conn, if_exists='replace', index=False)
        conn.commit()
        conn.close()
        
        print(f"Imported {len(demographics_df)} demographic records into SQLite")
        return demographics_df
    
    def sql_join_demo(self):
        """
        Demonstrate SQL joins between ratings and demographics in SQLite.
        
        Returns:
            DataFrame with joined results
        """
        print("\nPerforming SQL join demo (users + demographics)...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Check if demographics table exists
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='demographics'")
        if not cursor.fetchone():
            print("Demographics table not found in database")
            conn.close()
            return None
        
        # Perform SQL join
        query = """
        SELECT u.user_id, u.age, u.gender, u.occupation, 
               d.income_level, d.education_level
        FROM users u
        LEFT JOIN demographics d ON u.user_id = d.user_id
        LIMIT 10
        """
        
        result_df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"SQL join completed, retrieved {len(result_df)} records")
        return result_df
    
    def create_checkpoint(self, data_dict, checkpoint_name):
        """
        Create checkpoint to simulate fault tolerance.
        
        Args:
            data_dict: Dictionary of DataFrames to checkpoint
            checkpoint_name: Name of the checkpoint
        """
        checkpoint_dir = Path('data/checkpoints')
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"\nCreating checkpoint: {checkpoint_name}...")
        
        for name, df in data_dict.items():
            filepath = checkpoint_dir / f"{checkpoint_name}_{name}.csv"
            df.to_csv(filepath, index=False)
        
        print(f"Checkpoint '{checkpoint_name}' created successfully")
    
    def run_etl_pipeline(self):
        """
        Execute the complete ETL pipeline with pandas.
        
        Returns:
            Dictionary containing all processed data
        """
        print("=" * 60)
        print("Starting Pandas ETL Pipeline")
        print("=" * 60)
        
        start_time = time.time()
        
        # Load data
        ratings_df, users_df, items_df = self.load_data()
        
        # Create SQLite database
        self.create_sqlite_db(users_df)
        
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
