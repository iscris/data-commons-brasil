import glob
import os

import pandas as pd


def identify_columns(df):
    """Automatically identify the columns for date, place, and observation."""
    date_col = next(col for col in df.columns if "date" in col.lower())
    place_col = next(col for col in df.columns if "place" in col.lower())
    # Assuming the observation column is the one left over
    observation_cols = [col for col in df.columns if col not in [date_col, place_col]]
    if len(observation_cols) != 1:
        raise ValueError("Could not uniquely identify the observation column.")
    return date_col, place_col, observation_cols[0]


def process_csv_files(directory):
    # Find all CSV files in the specified directory
    csv_files = glob.glob(os.path.join(directory, "*.csv"))

    for filepath in csv_files:
        df = pd.read_csv(filepath)

        # Identify columns
        date_col, place_col, observation_col = identify_columns(df)

        # Convert the 'date' column to datetime, assuming the format is 'YYYY-MM-DD-HH-MM'
        df[date_col] = pd.to_datetime(df[date_col], format="%Y-%m-%d-%H-%M")

        # Convert datetime to date (YYYY-MM-DD ISO format)
        df[date_col] = df[date_col].dt.date

        # Group by 'date' and 'place', calculating the mean of the observation values
        df_aggregated = (
            df.groupby([place_col, date_col])
            .agg({observation_col: "mean"})
            .reset_index()
        )

        # Round the observation column to 1 decimal place
        df_aggregated[observation_col] = df_aggregated[observation_col].round(2)

        # Reorder columns: 'place', 'date', 'observation'
        df_aggregated = df_aggregated[[place_col, date_col, observation_col]]

        # Save the processed dataframe back to CSV
        output_filepath = os.path.splitext(filepath)[0] + "_processed.csv"
        df_aggregated.to_csv(output_filepath, index=False)

        print(f"Processed file saved to {output_filepath}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Process CSV files in a directory, identify columns by name, calculate the mean of observations rounded to 1 decimal place, and reorder columns."
    )
    parser.add_argument(
        "directory", type=str, help="Directory containing CSV files to process."
    )
    args = parser.parse_args()

    process_csv_files(args.directory)


if __name__ == "__main__":
    main()
