"""
Reporting Module

This module handles generation and export of summary reports for the MovieLens
analytics pipeline, including top genres, active users, and comparison statistics.
"""

import pandas as pd
from pathlib import Path
import json


class ReportGenerator:
    """Handles report generation and export operations."""
    
    def __init__(self, output_dir='data/reports'):
        """
        Initialize the report generator.
        
        Args:
            output_dir: Directory for report output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_top_genres_report(self, genre_popularity_df):
        """
        Generate report of top genres by popularity.
        
        Args:
            genre_popularity_df: DataFrame with genre popularity statistics
            
        Returns:
            Path to the exported CSV file
        """
        print("\nGenerating top genres report...")
        
        # Sort by number of ratings (descending)
        top_genres = genre_popularity_df.sort_values('num_ratings', ascending=False)
        
        # Export to CSV
        output_path = self.output_dir / 'top_genres.csv'
        top_genres.to_csv(output_path, index=False)
        
        print(f"Top genres report saved to {output_path}")
        print("\nTop 5 Genres by Number of Ratings:")
        print(top_genres.head().to_string(index=False))
        
        return output_path
    
    def generate_active_users_report(self, ratings_df, users_df, min_ratings=50):
        """
        Generate report of most active users.
        
        Args:
            ratings_df: DataFrame with ratings
            users_df: DataFrame with users
            min_ratings: Minimum ratings threshold
            
        Returns:
            Path to the exported CSV file
        """
        print("\nGenerating active users report...")
        
        # Calculate user activity
        user_activity = ratings_df.groupby('user_id').agg({
            'rating': ['count', 'mean']
        }).reset_index()
        
        user_activity.columns = ['user_id', 'num_ratings', 'avg_rating']
        
        # Join with user demographics
        active_users = user_activity.merge(users_df, on='user_id', how='left')
        
        # Filter active users
        active_users = active_users[active_users['num_ratings'] >= min_ratings]
        
        # Sort by number of ratings
        active_users = active_users.sort_values('num_ratings', ascending=False)
        
        # Export to CSV
        output_path = self.output_dir / 'active_users.csv'
        active_users.to_csv(output_path, index=False)
        
        print(f"Active users report saved to {output_path}")
        print(f"\nTop 10 Most Active Users (min {min_ratings} ratings):")
        print(active_users.head(10).to_string(index=False))
        
        return output_path
    
    def generate_movie_recommendations_report(self, movie_stats_df, top_n=20):
        """
        Generate report of top recommended movies.
        
        Args:
            movie_stats_df: DataFrame with movie statistics
            top_n: Number of top movies to include
            
        Returns:
            Path to the exported CSV file
        """
        print("\nGenerating movie recommendations report...")
        
        # Filter movies with at least 50 ratings for reliability
        popular_movies = movie_stats_df[movie_stats_df['rating_count'] >= 50]
        
        # Sort by average rating
        top_movies = popular_movies.sort_values('avg_rating', ascending=False).head(top_n)
        
        # Export to CSV
        output_path = self.output_dir / 'top_movies.csv'
        top_movies.to_csv(output_path, index=False)
        
        print(f"Movie recommendations report saved to {output_path}")
        print(f"\nTop 10 Recommended Movies:")
        print(top_movies.head(10).to_string(index=False))
        
        return output_path
    
    def generate_performance_comparison_report(self, pandas_time, spark_time):
        """
        Generate report comparing pandas vs Spark performance.
        
        Args:
            pandas_time: Processing time for pandas operations (seconds)
            spark_time: Processing time for Spark operations (seconds)
            
        Returns:
            Path to the exported report
        """
        print("\nGenerating performance comparison report...")
        
        comparison = {
            'Framework': ['Pandas', 'PySpark'],
            'Processing Time (seconds)': [pandas_time, spark_time],
            'Relative Performance': [1.0, pandas_time / spark_time if spark_time > 0 else 0]
        }
        
        comparison_df = pd.DataFrame(comparison)
        
        # Export to CSV
        output_path = self.output_dir / 'performance_comparison.csv'
        comparison_df.to_csv(output_path, index=False)
        
        print(f"Performance comparison saved to {output_path}")
        print("\n" + "=" * 60)
        print("PERFORMANCE COMPARISON: Pandas vs. PySpark")
        print("=" * 60)
        print(comparison_df.to_string(index=False))
        print("=" * 60)
        
        # Analysis
        if spark_time < pandas_time:
            speedup = pandas_time / spark_time
            print(f"\nPySpark is {speedup:.2f}x faster than Pandas for this workload")
        else:
            slowdown = spark_time / pandas_time
            print(f"\nPandas is {slowdown:.2f}x faster than PySpark for this workload")
            print("(Note: PySpark overhead may dominate for small datasets)")
        
        return output_path
    
    def generate_summary_statistics(self, ratings_df, users_df, items_df):
        """
        Generate overall summary statistics report.
        
        Args:
            ratings_df: DataFrame with ratings
            users_df: DataFrame with users
            items_df: DataFrame with items
            
        Returns:
            Dictionary with summary statistics
        """
        print("\nGenerating summary statistics...")
        
        summary = {
            'total_ratings': len(ratings_df),
            'total_users': len(users_df),
            'total_movies': len(items_df),
            'avg_rating': ratings_df['rating'].mean(),
            'median_rating': ratings_df['rating'].median(),
            'rating_std': ratings_df['rating'].std(),
            'ratings_per_user': len(ratings_df) / len(users_df),
            'ratings_per_movie': len(ratings_df) / len(items_df),
            'sparsity': 1 - (len(ratings_df) / (len(users_df) * len(items_df)))
        }
        
        # Export to JSON
        output_path = self.output_dir / 'summary_statistics.json'
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"Summary statistics saved to {output_path}")
        
        print("\n" + "=" * 60)
        print("DATASET SUMMARY STATISTICS")
        print("=" * 60)
        for key, value in summary.items():
            if isinstance(value, float):
                print(f"{key.replace('_', ' ').title()}: {value:.4f}")
            else:
                print(f"{key.replace('_', ' ').title()}: {value}")
        print("=" * 60)
        
        return summary
    
    def generate_all_reports(self, pandas_results, spark_results):
        """
        Generate all reports from pipeline results.
        
        Args:
            pandas_results: Dictionary with pandas processing results
            spark_results: Dictionary with Spark processing results
            
        Returns:
            Dictionary with paths to all generated reports
        """
        print("=" * 60)
        print("Generating All Reports")
        print("=" * 60)
        
        report_paths = {}
        
        # Generate individual reports
        if 'genre_popularity' in pandas_results and pandas_results['genre_popularity'] is not None:
            report_paths['top_genres'] = self.generate_top_genres_report(
                pandas_results['genre_popularity']
            )
        
        if 'ratings' in pandas_results and 'users' in pandas_results:
            report_paths['active_users'] = self.generate_active_users_report(
                pandas_results['ratings'],
                pandas_results['users']
            )
        
        if 'movie_stats' in pandas_results:
            report_paths['top_movies'] = self.generate_movie_recommendations_report(
                pandas_results['movie_stats']
            )
        
        # Generate performance comparison
        pandas_time = pandas_results.get('processing_time', 0)
        spark_time = spark_results.get('processing_time', 0)
        report_paths['performance'] = self.generate_performance_comparison_report(
            pandas_time,
            spark_time
        )
        
        # Generate summary statistics
        if 'ratings' in pandas_results:
            summary = self.generate_summary_statistics(
                pandas_results['ratings'],
                pandas_results['users'],
                pandas_results['items']
            )
            report_paths['summary'] = self.output_dir / 'summary_statistics.json'
        
        print("\n" + "=" * 60)
        print("All Reports Generated Successfully")
        print("=" * 60)
        print("\nReport Files:")
        for report_name, path in report_paths.items():
            print(f"  - {report_name}: {path}")
        
        return report_paths


if __name__ == "__main__":
    # Test report generation with sample data
    from process import DataProcessor
    
    processor = DataProcessor()
    pandas_results = processor.run_etl_pipeline()
    
    reporter = ReportGenerator()
    
    # Generate sample reports
    if pandas_results['genre_popularity'] is not None:
        reporter.generate_top_genres_report(pandas_results['genre_popularity'])
    
    reporter.generate_active_users_report(
        pandas_results['ratings'],
        pandas_results['users']
    )
    
    reporter.generate_summary_statistics(
        pandas_results['ratings'],
        pandas_results['users'],
        pandas_results['items']
    )
