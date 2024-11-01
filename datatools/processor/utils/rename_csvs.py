import os


def add_csv_extension(directory):
    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        # Construct full file path
        file_path = os.path.join(directory, filename)

        # Check if it's a file (not a directory) and does not have .csv extension
        if os.path.isfile(file_path) and not filename.endswith(".csv"):
            # Construct new file name with .csv extension
            new_filename = filename + ".csv"
            new_file_path = os.path.join(directory, new_filename)

            # Rename the file
            os.rename(file_path, new_file_path)
            print(f"Renamed: {filename} -> {new_filename}")


# Specify the directory
directory_path = "../data_social_final"

# Call the function
add_csv_extension(directory_path)
