import json


# Define the function to parse the JSON file
def parse_json(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    # Filter out inputFiles with entityType as None or null
    filtered_input_files = {
        k: v for k, v in data["inputFiles"].items() if v["entityType"] is not None
    }
    data["inputFiles"] = filtered_input_files

    return data


# Function to save the filtered JSON data back to a file (optional)
def save_filtered_json(data, output_file_path):
    with open(output_file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


# Path to your JSON file
input_file_path = "../data_social_final/config.json"
output_file_path = "../data_social_final/config.json"

# Parse the JSON file and filter the data
filtered_data = parse_json(input_file_path)

# Optionally save the filtered data to a new JSON file
save_filtered_json(filtered_data, output_file_path)

# Print the filtered data
print(json.dumps(filtered_data, indent=4, ensure_ascii=False))
