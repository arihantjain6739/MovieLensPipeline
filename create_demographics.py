"""
Generate mock demographics CSV for integration demo.
This simulates external demographic data that can be joined with user data.
"""

import pandas as pd
import random

# Set seed for reproducibility
random.seed(42)

# Generate demographics for a subset of users (1-200)
num_users = 200

data = {
    'user_id': range(1, num_users + 1),
    'income_level': [random.choice(['Low', 'Medium', 'High', 'Very High']) for _ in range(num_users)],
    'education_level': [random.choice(['High School', 'Bachelor', 'Master', 'PhD']) for _ in range(num_users)]
}

demographics_df = pd.DataFrame(data)

# Save to CSV
output_path = 'data/processed/demographics.csv'
demographics_df.to_csv(output_path, index=False)

print(f"Created demographics CSV with {len(demographics_df)} records at {output_path}")
print("\nSample data:")
print(demographics_df.head(10))
