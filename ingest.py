"""
Data Ingestion Module for MovieLens 100k Dataset

This module handles downloading, extracting, and loading the MovieLens 100k 
dataset from grouplens.org into pandas DataFrames, then stores them in HDFS.
"""

import os
import zipfile
import requests
import pandas as pd
from pathlib import Path
import time
import subprocess
import tempfile


class MovieLensIngester:
    """Handles ingestion of MovieLens 100k dataset with HDFS support."""
    
    def __init__(self, data_dir='data', raw_dir='data/raw', processed_dir='data/processed', 
                 use_hdfs=False, hdfs_url='hdfs://localhost:9000'):
        """
        Initialize the ingester with directory paths.
        
        Args:
            data_dir: Root data directory
            raw_dir: Directory for raw downloaded data
            processed_dir: Directory for processed CSV files
            use_hdfs: Whether to use HDFS for storage
            hdfs_url: HDFS namenode URL
        """
        self.data_dir = Path(data_dir)
        self.raw_dir = Path(raw_dir)
        self.use_hdfs = use_hdfs
        self.hdfs_url = hdfs_url
        
        if use_hdfs:
            # HDFS paths
            self.processed_dir = '/movielens/data'
            print(f"Using HDFS storage: {hdfs_url}{self.processed_dir}")
        else:
            # Local filesystem
            self.processed_dir = Path(processed_dir)
            self.processed_dir.mkdir(parents=True, exist_ok=True)
            
        self.dataset_url = 'https://files.grouplens.org/datasets/movielens/ml-100k.zip'
        self.dataset_name = 'ml-100k'
        
        # Create local directories if they don't exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
    
    def download_dataset(self):
        """Download MovieLens 100k dataset from grouplens.org."""
        zip_path = self.raw_dir / 'ml-100k.zip'
        
        if zip_path.exists():
            print(f"Dataset already downloaded at {zip_path}")
            return zip_path
        
        print(f"Downloading MovieLens 100k dataset from {self.dataset_url}...")
        start_time = time.time()
        
        response = requests.get(self.dataset_url, stream=True)
        response.raise_for_status()
        
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        elapsed = time.time() - start_time
        print(f"Download completed in {elapsed:.2f} seconds")
        return zip_path
    
    def extract_dataset(self, zip_path):
        """
        Extract the downloaded zip file.
        
        Args:
            zip_path: Path to the zip file
        """
        extract_path = self.raw_dir / self.dataset_name
        
        if extract_path.exists():
            print(f"Dataset already extracted at {extract_path}")
            return extract_path
        
        print(f"Extracting dataset to {self.raw_dir}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(self.raw_dir)
        
        print("Extraction completed")
        return extract_path
    
    def load_ratings(self):
        """
        Load ratings data into pandas DataFrame.
        
        Returns:
            DataFrame with columns: user_id, item_id, rating, timestamp
        """
        ratings_path = self.raw_dir / self.dataset_name / 'u.data'
        print(f"Loading ratings from {ratings_path}...")
        
        df = pd.read_csv(
            ratings_path,
            sep='\t',
            names=['user_id', 'item_id', 'rating', 'timestamp'],
            encoding='latin-1'
        )
        print(f"Loaded {len(df)} ratings")
        return df
    
    def load_users(self):
        """
        Load users data into pandas DataFrame.
        
        Returns:
            DataFrame with columns: user_id, age, gender, occupation, zip_code
        """
        users_path = self.raw_dir / self.dataset_name / 'u.user'
        print(f"Loading users from {users_path}...")
        
        df = pd.read_csv(
            users_path,
            sep='|',
            names=['user_id', 'age', 'gender', 'occupation', 'zip_code'],
            encoding='latin-1'
        )
        print(f"Loaded {len(df)} users")
        return df
    
    def load_items(self):
        """
        Load items (movies) data into pandas DataFrame.
        
        Returns:
            DataFrame with movie information including genres
        """
        items_path = self.raw_dir / self.dataset_name / 'u.item'
        print(f"Loading items from {items_path}...")
        
        # Define column names based on MovieLens 100k documentation
        columns = ['item_id', 'title', 'release_date', 'video_release_date', 'imdb_url',
                   'unknown', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy',
                   'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror',
                   'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
        
        df = pd.read_csv(
            items_path,
            sep='|',
            names=columns,
            encoding='latin-1'
        )
        print(f"Loaded {len(df)} items")
        return df
    
    def save_to_csv(self, df, filename):
        """
        Save DataFrame to CSV file in local filesystem or HDFS.
        
        Args:
            df: pandas DataFrame to save
            filename: Name of the CSV file
        """
        if self.use_hdfs:
            # Save to HDFS using docker exec
            print(f"Saving to HDFS: {self.hdfs_url}{self.processed_dir}/{filename}...")
            
            # Write to temporary file first
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp:
                df.to_csv(tmp.name, index=False)
                tmp_path = tmp.name
            
            # Upload to HDFS
            hdfs_path = f"{self.processed_dir}/{filename}"
            try:
                subprocess.run([
                    'docker', 'exec', '-i', 'namenode',
                    'hdfs', 'dfs', '-put', '-f', '-', hdfs_path
                ], stdin=open(tmp_path, 'rb'), check=True, capture_output=True)
                print(f"✓ Saved {len(df)} records to HDFS: {hdfs_path}")
            except subprocess.CalledProcessError as e:
                print(f"Error uploading to HDFS: {e.stderr.decode()}")
                raise
            finally:
                # Clean up temp file
                os.unlink(tmp_path)
        else:
            # Save to local filesystem
            filepath = self.processed_dir / filename
            print(f"Saving to {filepath}...")
            df.to_csv(filepath, index=False)
            print(f"Saved {len(df)} records to {filepath}")
    
    def ingest_all(self):
        """
        Main ingestion workflow: download, extract, load, and save all data.
        
        Returns:
            Tuple of (ratings_df, users_df, items_df)
        """
        print("=" * 60)
        print("Starting MovieLens 100k Data Ingestion")
        if self.use_hdfs:
            print(f"Storage Backend: HDFS ({self.hdfs_url})")
        else:
            print("Storage Backend: Local Filesystem")
        print("=" * 60)
        
        # Download and extract
        zip_path = self.download_dataset()
        self.extract_dataset(zip_path)
        
        # Load into pandas DataFrames
        ratings_df = self.load_ratings()
        users_df = self.load_users()
        items_df = self.load_items()
        
        # Save to storage backend (HDFS or local)
        storage_type = "HDFS" if self.use_hdfs else "CSV files"
        print(f"\nStoring data in {storage_type}...")
        self.save_to_csv(ratings_df, 'ratings.csv')
        self.save_to_csv(users_df, 'users.csv')
        self.save_to_csv(items_df, 'items.csv')
        
        print("\n" + "=" * 60)
        print("Data Ingestion Completed Successfully")
        print("=" * 60)
        
        return ratings_df, users_df, items_df


if __name__ == "__main__":
    # Test the ingestion module
    ingester = MovieLensIngester()
    ratings_df, users_df, items_df = ingester.ingest_all()
    
    # Display sample data
    print("\nSample Ratings:")
    print(ratings_df.head())
    print("\nSample Users:")
    print(users_df.head())
    print("\nSample Items:")
    print(items_df.head())
