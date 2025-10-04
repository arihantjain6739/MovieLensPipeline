"""
Main Pipeline Orchestrator for MovieLens 100k Big Data Demo

This script orchestrates the entire data pipeline including:
1. Data ingestion from grouplens.org
2. Pandas-based ETL processing
3. PySpark analytics jobs
4. Report generation and export
5. Performance benchmarking
"""

import sys
from pathlib import Path

from ingest import MovieLensIngester
from process import DataProcessor
from spark_jobs import SparkAnalytics
from report import ReportGenerator


def print_banner(text):
    """Print a formatted banner."""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)


def main():
    """Execute the complete Big Data pipeline."""
    
    print_banner("MOVIELENS 100K BIG DATA PIPELINE")
    print("Demonstrating end-to-end data pipeline with Pandas, PySpark, and SQLite")
    print("=" * 70)
    
    try:
        # Step 1: Data Ingestion
        print_banner("STEP 1: DATA INGESTION")
        ingester = MovieLensIngester()
        ratings_df, users_df, items_df = ingester.ingest_all()
        
        # Step 2: Pandas ETL Processing
        print_banner("STEP 2: PANDAS ETL PROCESSING")
        processor = DataProcessor()
        pandas_results = processor.run_etl_pipeline()
        
        # Step 3: PySpark Analytics
        print_banner("STEP 3: PYSPARK ANALYTICS")
        analytics = SparkAnalytics()
        
        try:
            spark_results = analytics.run_spark_analytics()
        finally:
            # Always stop Spark session
            analytics.stop_spark()
        
        # Step 4: Report Generation
        print_banner("STEP 4: REPORT GENERATION")
        reporter = ReportGenerator()
        report_paths = reporter.generate_all_reports(pandas_results, spark_results)
        
        # Step 5: Final Summary
        print_banner("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        
        print("\n📊 PIPELINE SUMMARY:")
        print(f"  ✓ Ingested {len(ratings_df)} ratings from {len(users_df)} users")
        print(f"  ✓ Analyzed {len(items_df)} movies across multiple genres")
        print(f"  ✓ Pandas processing time: {pandas_results['processing_time']:.2f}s")
        print(f"  ✓ Spark processing time: {spark_results['processing_time']:.2f}s")
        print(f"  ✓ Generated {len(report_paths)} comprehensive reports")
        
        print("\n📁 OUTPUT LOCATIONS:")
        print(f"  • Processed Data: data/processed/")
        print(f"  • Spark Output: data/spark_output/")
        print(f"  • Reports: data/reports/")
        print(f"  • Checkpoints: data/checkpoints/")
        print(f"  • SQLite Database: data/movielens.db")
        
        print("\n🎯 KEY INSIGHTS:")
        
        # Top genre
        if pandas_results['genre_popularity'] is not None and len(pandas_results['genre_popularity']) > 0:
            top_genre = pandas_results['genre_popularity'].iloc[0]
            print(f"  • Most popular genre: {top_genre['genre']} ({top_genre['num_ratings']} ratings)")
        
        # CF Model performance
        if 'cf_rmse' in spark_results:
            print(f"  • Collaborative filtering RMSE: {spark_results['cf_rmse']:.4f}")
        
        # Performance comparison
        if pandas_results['processing_time'] > 0 and spark_results['processing_time'] > 0:
            ratio = spark_results['processing_time'] / pandas_results['processing_time']
            if ratio < 1:
                print(f"  • Spark was {1/ratio:.2f}x faster than Pandas")
            else:
                print(f"  • Pandas was {ratio:.2f}x faster than Spark (overhead for small data)")
        
        print("\n" + "=" * 70)
        print("  🎉 Big Data Pipeline Demo Completed Successfully!")
        print("=" * 70)
        
        return 0
        
    except Exception as e:
        print(f"\n❌ ERROR: Pipeline execution failed")
        print(f"Error details: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
