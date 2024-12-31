import pandas as pd
import random

# Function to filter the data and randomly select 250k rows
def filter_and_select(input_file, output_file, quarter_col='quarter', target_quarter='Quarter 3', num_samples=250000):
    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(input_file)
    
    # Filter the rows where the quarter column is 'Quarter 3'
    filtered_df = df[df["Quarter"] == 3]
    
    # Check if there are enough rows to sample
    if len(filtered_df) < num_samples:
        print(f"Warning: Only {len(filtered_df)} rows available. Selecting all of them.")
        num_samples = len(filtered_df)
    
    # Randomly sample the specified number of rows
    sampled_df = filtered_df.sample(n=num_samples, random_state=42)
    
    # Export the sampled rows to a new CSV file
    sampled_df.to_csv(output_file, index=False)
    print(f"Exported {num_samples} rows to {output_file}")

# Example usage
input_csv = 'Cleaned_2018_Flights.csv'  # Replace with the path to your input CSV file
output_csv = 'voos_2018_q3_250k.csv'  # Replace with the desired output file path

filter_and_select(input_csv, output_csv)