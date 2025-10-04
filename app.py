"""
MovieLens 100k Interactive Dashboard

A comprehensive Streamlit dashboard for exploring MovieLens dataset analytics
with multiple pages including Overview, Dataset Explorer, Analytics Results,
Performance Comparison, and User Insights.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import json
import sqlite3

st.set_page_config(
    page_title="MovieLens Analytics Dashboard",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_data():
    """Load all data files for the dashboard."""
    data = {}
    
    try:
        data['ratings'] = pd.read_csv('data/processed/ratings.csv')
        data['users'] = pd.read_csv('data/processed/users.csv')
        data['items'] = pd.read_csv('data/processed/items.csv')
        data['top_genres'] = pd.read_csv('data/reports/top_genres.csv')
        data['active_users'] = pd.read_csv('data/reports/active_users.csv')
        data['top_movies'] = pd.read_csv('data/reports/top_movies.csv')
        data['performance'] = pd.read_csv('data/reports/performance_comparison.csv')
        data['spark_top_movies'] = pd.read_csv('data/spark_output/top_movies_per_genre.csv')
        data['user_segments'] = pd.read_csv('data/spark_output/user_segment_summary.csv')
        
        with open('data/reports/summary_statistics.json', 'r') as f:
            data['summary'] = json.load(f)
            
    except FileNotFoundError as e:
        st.error(f"Data files not found. Please run the pipeline first: `python main.py`")
        st.stop()
    
    return data

def show_overview(data):
    """Display overview page with key metrics and summary."""
    st.title("🎬 MovieLens 100k Analytics Dashboard")
    st.markdown("### Overview of Dataset and Pipeline Results")
    
    summary = data['summary']
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Ratings", f"{summary['total_ratings']:,}")
    with col2:
        st.metric("Total Users", f"{summary['total_users']:,}")
    with col3:
        st.metric("Total Movies", f"{summary['total_movies']:,}")
    with col4:
        st.metric("Average Rating", f"{summary['avg_rating']:.2f}")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Dataset Statistics")
        stats_df = pd.DataFrame({
            'Metric': ['Median Rating', 'Rating Std Dev', 'Ratings per User', 'Ratings per Movie', 'Sparsity'],
            'Value': [
                f"{summary['median_rating']:.2f}",
                f"{summary['rating_std']:.4f}",
                f"{summary['ratings_per_user']:.2f}",
                f"{summary['ratings_per_movie']:.2f}",
                f"{summary['sparsity']:.2%}"
            ]
        })
        st.dataframe(stats_df, hide_index=True, use_container_width=True)
    
    with col2:
        st.subheader("🎯 Top 5 Genres by Popularity")
        top_5_genres = data['top_genres'].head(5)
        fig = px.bar(
            top_5_genres,
            x='genre',
            y='num_ratings',
            title='Most Popular Genres',
            labels={'num_ratings': 'Number of Ratings', 'genre': 'Genre'},
            color='avg_rating',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    st.subheader("⭐ Top 10 Highest Rated Movies")
    top_10 = data['top_movies'].head(10)[['title', 'avg_rating', 'rating_count']]
    top_10.columns = ['Movie Title', 'Average Rating', 'Number of Ratings']
    st.dataframe(top_10, hide_index=True, use_container_width=True)

def show_dataset_explorer(data):
    """Display dataset explorer page for raw data exploration."""
    st.title("🔍 Dataset Explorer")
    st.markdown("### Explore Raw MovieLens Data")
    
    tab1, tab2, tab3 = st.tabs(["📊 Ratings", "👥 Users", "🎬 Movies"])
    
    with tab1:
        st.subheader("Ratings Data")
        st.markdown(f"**Total Ratings:** {len(data['ratings']):,}")
        
        col1, col2 = st.columns(2)
        
        with col1:
            rating_dist = data['ratings']['rating'].value_counts().sort_index()
            fig = px.bar(
                x=rating_dist.index,
                y=rating_dist.values,
                title='Distribution of Ratings',
                labels={'x': 'Rating', 'y': 'Count'},
                color=rating_dist.values,
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            ratings_by_user = data['ratings'].groupby('user_id').size()
            fig = px.histogram(
                ratings_by_user,
                nbins=50,
                title='Distribution of Ratings per User',
                labels={'value': 'Number of Ratings', 'count': 'Number of Users'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("#### Sample Ratings Data")
        st.dataframe(data['ratings'].head(100), use_container_width=True)
    
    with tab2:
        st.subheader("Users Data")
        st.markdown(f"**Total Users:** {len(data['users']):,}")
        
        col1, col2 = st.columns(2)
        
        with col1:
            age_dist = data['users']['age'].value_counts().sort_index()
            fig = px.histogram(
                data['users'],
                x='age',
                nbins=30,
                title='Age Distribution of Users',
                labels={'age': 'Age', 'count': 'Number of Users'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            gender_dist = data['users']['gender'].value_counts()
            fig = px.pie(
                values=gender_dist.values,
                names=gender_dist.index,
                title='Gender Distribution',
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("#### Occupation Distribution")
        occupation_dist = data['users']['occupation'].value_counts().head(10)
        fig = px.bar(
            x=occupation_dist.values,
            y=occupation_dist.index,
            orientation='h',
            title='Top 10 Occupations',
            labels={'x': 'Count', 'y': 'Occupation'}
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("#### Sample Users Data")
        st.dataframe(data['users'].head(50), use_container_width=True)
    
    with tab3:
        st.subheader("Movies Data")
        st.markdown(f"**Total Movies:** {len(data['items']):,}")
        
        genre_columns = ['Action', 'Adventure', 'Animation', 'Children', 'Comedy',
                        'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir',
                        'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi',
                        'Thriller', 'War', 'Western']
        
        genre_counts = {}
        for genre in genre_columns:
            if genre in data['items'].columns:
                genre_counts[genre] = data['items'][genre].sum()
        
        genre_df = pd.DataFrame(list(genre_counts.items()), columns=['Genre', 'Count'])
        genre_df = genre_df.sort_values('Count', ascending=False)
        
        fig = px.bar(
            genre_df,
            x='Genre',
            y='Count',
            title='Number of Movies per Genre',
            labels={'Count': 'Number of Movies', 'Genre': 'Genre'}
        )
        fig.update_xaxis(tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("#### Sample Movies Data")
        display_cols = ['item_id', 'title', 'release_date'] + [g for g in genre_columns if g in data['items'].columns][:5]
        st.dataframe(data['items'][display_cols].head(50), use_container_width=True)

def show_analytics_results(data):
    """Display analytics results from Pandas and Spark processing."""
    st.title("📈 Analytics Results")
    st.markdown("### Insights from Pandas and PySpark Analytics")
    
    tab1, tab2, tab3 = st.tabs(["🎭 Genre Analytics", "⭐ Movie Rankings", "🔥 User Activity"])
    
    with tab1:
        st.subheader("Genre Popularity Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                data['top_genres'],
                x='genre',
                y='num_ratings',
                title='Genres by Number of Ratings',
                labels={'num_ratings': 'Number of Ratings', 'genre': 'Genre'},
                color='avg_rating',
                color_continuous_scale='RdYlGn'
            )
            fig.update_xaxis(tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.scatter(
                data['top_genres'],
                x='num_movies',
                y='avg_rating',
                size='num_ratings',
                hover_data=['genre'],
                title='Genre Quality vs Quantity',
                labels={'num_movies': 'Number of Movies', 'avg_rating': 'Average Rating'},
                color='num_ratings',
                color_continuous_scale='Plasma'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("#### Genre Statistics Table")
        st.dataframe(data['top_genres'], hide_index=True, use_container_width=True)
        
        st.markdown("---")
        st.subheader("Top Movies by Genre (PySpark Analysis)")
        
        selected_genre = st.selectbox(
            "Select a genre to view top movies:",
            sorted(data['spark_top_movies']['genre'].unique())
        )
        
        genre_movies = data['spark_top_movies'][data['spark_top_movies']['genre'] == selected_genre]
        genre_movies = genre_movies.sort_values('avg_rating', ascending=False)
        
        st.dataframe(
            genre_movies[['title', 'avg_rating', 'num_ratings']],
            hide_index=True,
            use_container_width=True
        )
    
    with tab2:
        st.subheader("Movie Rankings and Recommendations")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            top_n = st.slider("Number of movies to display:", 5, 50, 20)
            top_movies_display = data['top_movies'].head(top_n)
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=top_movies_display['title'][::-1],
                x=top_movies_display['avg_rating'][::-1],
                orientation='h',
                marker=dict(
                    color=top_movies_display['rating_count'][::-1],
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Rating Count")
                ),
                text=top_movies_display['avg_rating'][::-1].round(2),
                textposition='auto',
            ))
            fig.update_layout(
                title=f'Top {top_n} Movies by Average Rating',
                xaxis_title='Average Rating',
                yaxis_title='Movie',
                height=max(400, top_n * 20)
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### Filter Options")
            min_ratings = st.number_input(
                "Minimum number of ratings:",
                min_value=1,
                max_value=500,
                value=50,
                step=10
            )
            
            filtered_movies = data['top_movies'][data['top_movies']['rating_count'] >= min_ratings]
            st.metric("Filtered Movies", len(filtered_movies))
            
            st.markdown("#### Top Movie Statistics")
            if len(filtered_movies) > 0:
                st.write(f"**Highest Rated:** {filtered_movies.iloc[0]['title']}")
                st.write(f"**Rating:** {filtered_movies.iloc[0]['avg_rating']:.2f}")
                st.write(f"**Ratings Count:** {filtered_movies.iloc[0]['rating_count']:.0f}")
    
    with tab3:
        st.subheader("User Activity and Engagement")
        
        col1, col2 = st.columns(2)
        
        with col1:
            activity_dist = data['active_users'].groupby('occupation')['num_ratings'].mean().sort_values(ascending=False).head(10)
            fig = px.bar(
                x=activity_dist.values,
                y=activity_dist.index,
                orientation='h',
                title='Average Ratings by Occupation (Top 10)',
                labels={'x': 'Average Number of Ratings', 'y': 'Occupation'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.scatter(
                data['active_users'].head(100),
                x='num_ratings',
                y='avg_rating',
                color='gender',
                hover_data=['age', 'occupation'],
                title='User Rating Behavior (Top 100 Active Users)',
                labels={'num_ratings': 'Number of Ratings', 'avg_rating': 'Average Rating Given'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("#### Most Active Users")
        display_cols = ['user_id', 'num_ratings', 'avg_rating', 'age', 'gender', 'occupation']
        st.dataframe(
            data['active_users'][display_cols].head(20),
            hide_index=True,
            use_container_width=True
        )

def show_performance_comparison(data):
    """Display performance comparison between Pandas and PySpark."""
    st.title("⚡ Performance Comparison")
    st.markdown("### Pandas vs PySpark Processing Time")
    
    perf_data = data['performance']
    
    col1, col2, col3 = st.columns(3)
    
    pandas_time = perf_data[perf_data['Framework'] == 'Pandas']['Processing Time (seconds)'].values[0]
    spark_time = perf_data[perf_data['Framework'] == 'PySpark']['Processing Time (seconds)'].values[0]
    
    with col1:
        st.metric("Pandas Processing Time", f"{pandas_time:.2f}s")
    with col2:
        st.metric("PySpark Processing Time", f"{spark_time:.2f}s")
    with col3:
        if pandas_time < spark_time:
            speedup = spark_time / pandas_time
            st.metric("Winner", "Pandas", f"{speedup:.2f}x faster")
        else:
            speedup = pandas_time / spark_time
            st.metric("Winner", "PySpark", f"{speedup:.2f}x faster")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            perf_data,
            x='Framework',
            y='Processing Time (seconds)',
            title='Processing Time Comparison',
            labels={'Processing Time (seconds)': 'Time (seconds)'},
            color='Framework',
            color_discrete_map={'Pandas': '#2E86AB', 'PySpark': '#A23B72'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = go.Figure(data=[go.Pie(
            labels=perf_data['Framework'],
            values=perf_data['Processing Time (seconds)'],
            hole=.3,
            marker_colors=['#2E86AB', '#A23B72']
        )])
        fig.update_layout(title='Processing Time Distribution')
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    st.subheader("📊 Performance Analysis")
    
    st.markdown("""
    #### Key Insights:
    
    **For the MovieLens 100k dataset (small dataset):**
    
    - **Pandas is significantly faster** for this workload due to minimal overhead
    - Pandas operates directly on in-memory data structures
    - PySpark has initialization and coordination overhead that dominates for small datasets
    
    **When to use each framework:**
    
    | Framework | Best For | Strengths |
    |-----------|----------|-----------|
    | **Pandas** | Small to medium datasets (<10GB) | Fast, simple API, rich ecosystem |
    | **PySpark** | Large datasets (>10GB), distributed computing | Horizontal scaling, fault tolerance, distributed ML |
    
    **Trade-offs:**
    
    - **Overhead:** PySpark requires JVM initialization and task scheduling
    - **Scalability:** PySpark scales horizontally across clusters
    - **Memory:** Pandas requires all data to fit in single machine memory
    - **Complexity:** Pandas has a simpler, more intuitive API
    
    For production big data pipelines with datasets larger than available RAM, PySpark would show significant advantages.
    """)

def show_user_insights(data):
    """Display user segmentation and behavior insights."""
    st.title("👥 User Insights")
    st.markdown("### User Segmentation and Behavior Analysis")
    
    st.subheader("User Segmentation by Rating Behavior")
    
    segments = data['user_segments']
    
    fig = px.sunburst(
        segments,
        path=['frequency_segment', 'rating_tendency'],
        values='user_count',
        title='User Segmentation Hierarchy',
        color='user_count',
        color_continuous_scale='RdYlGn'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            segments,
            x='frequency_segment',
            y='user_count',
            color='rating_tendency',
            title='User Count by Frequency Segment',
            labels={'user_count': 'Number of Users', 'frequency_segment': 'Rating Frequency'},
            barmode='group'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        tendency_total = segments.groupby('rating_tendency')['user_count'].sum().reset_index()
        fig = px.pie(
            tendency_total,
            values='user_count',
            names='rating_tendency',
            title='Rating Tendency Distribution',
            color='rating_tendency',
            color_discrete_map={'harsh': '#FF6B6B', 'neutral': '#FFD93D', 'generous': '#6BCB77'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    st.subheader("Segment Definitions")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        #### Frequency Segments
        - **Low:** < 50 ratings
        - **Medium:** 50-149 ratings
        - **High:** ≥ 150 ratings
        """)
    
    with col2:
        st.markdown("""
        #### Rating Tendency
        - **Harsh:** Average rating < 3.0
        - **Neutral:** Average rating 3.0-3.9
        - **Generous:** Average rating ≥ 4.0
        """)
    
    st.markdown("---")
    
    st.subheader("Demographic Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        age_groups = data['active_users'].copy()
        age_groups['age_group'] = pd.cut(
            age_groups['age'],
            bins=[0, 18, 25, 35, 50, 100],
            labels=['<18', '18-25', '25-35', '35-50', '50+']
        )
        age_activity = age_groups.groupby('age_group')['num_ratings'].mean().reset_index()
        
        fig = px.bar(
            age_activity,
            x='age_group',
            y='num_ratings',
            title='Average Activity by Age Group',
            labels={'num_ratings': 'Average Number of Ratings', 'age_group': 'Age Group'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        gender_activity = data['active_users'].groupby('gender')['num_ratings'].mean().reset_index()
        fig = px.bar(
            gender_activity,
            x='gender',
            y='num_ratings',
            title='Average Activity by Gender',
            labels={'num_ratings': 'Average Number of Ratings', 'gender': 'Gender'},
            color='gender'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("#### User Segment Summary Table")
    st.dataframe(segments, hide_index=True, use_container_width=True)

def main():
    """Main dashboard application."""
    
    st.sidebar.title("🎬 MovieLens Dashboard")
    st.sidebar.markdown("### Navigation")
    
    page = st.sidebar.radio(
        "Select Page:",
        ["Overview", "Dataset Explorer", "Analytics Results", "Performance Comparison", "User Insights"]
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    ### About
    This dashboard provides interactive visualizations 
    for the MovieLens 100k dataset analytics pipeline.
    
    **Dataset:** MovieLens 100k  
    **Source:** GroupLens Research
    
    **Technologies:**
    - 🐼 Pandas for ETL
    - ⚡ PySpark for distributed analytics
    - 📊 Plotly for visualizations
    - 🚀 Streamlit for dashboard
    """)
    
    try:
        data = load_data()
        
        if page == "Overview":
            show_overview(data)
        elif page == "Dataset Explorer":
            show_dataset_explorer(data)
        elif page == "Analytics Results":
            show_analytics_results(data)
        elif page == "Performance Comparison":
            show_performance_comparison(data)
        elif page == "User Insights":
            show_user_insights(data)
            
    except Exception as e:
        st.error(f"Error loading dashboard: {str(e)}")
        st.info("Please ensure the pipeline has been run first: `python main.py`")

if __name__ == "__main__":
    main()
