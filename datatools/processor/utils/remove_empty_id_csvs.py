import json
import os

import pandas as pd


def delete_csvs_with_null_wikidataId_and_update_config(directory, config_file):
    # Load the config file
    with open(config_file, "r", encoding="utf-8") as f:
        config = json.load(f)

    # List to keep track of deleted files
    deleted_files = []

    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            try:
                df = pd.read_csv(file_path)
                if "wikidataId" in df.columns:
                    if (
                        df["wikidataId"].isnull().any()
                        or (df["wikidataId"] == "").any()
                    ):
                        os.remove(file_path)
                        deleted_files.append(filename)
                        print(
                            f"Deleted {filename} because it contains null/NaN/empty-string in 'wikidataId' column."
                        )
                    else:
                        print(
                            f"{filename} is fine. No null/NaN/empty-string in 'wikidataId' column."
                        )
                else:
                    print(f"{filename} does not contain 'wikidataId' column.")
            except Exception as e:
                print(f"Failed to process {filename}. Error: {e}")

    # Update the config file by removing deleted files
    for file in deleted_files:
        if file in config["inputFiles"]:
            del config["inputFiles"][file]

    # Save the updated config file
    with open(config_file, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4, ensure_ascii=False)
        print(f"Updated {config_file} by removing entries for deleted files.")


# Directory containing the CSV files
directory_path = "../data_social_final/"
# Path to the config file
config_file_path = os.path.join(directory_path, "config.json")

# Call the function
delete_csvs_with_null_wikidataId_and_update_config(directory_path, config_file_path)
