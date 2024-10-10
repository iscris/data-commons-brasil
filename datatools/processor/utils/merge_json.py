import argparse
import json


def deep_merge(dict1, dict2):
    for key in dict2:
        if key in dict1:
            if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                deep_merge(dict1[key], dict2[key])
            else:
                dict1[key] = dict2[key]
        else:
            dict1[key] = dict2[key]
    return dict1


def shallow_merge(dict1, dict2):
    merged = json.copy()
    merged.update(dict2)

    return merged


def load_json(filepath):
    with open(filepath, "r") as file:
        return json.load(file)


def main():
    parser = argparse.ArgumentParser(description="Merge two JSON files.")
    parser.add_argument("json1_path", type=str, help="The path to the first JSON file.")
    parser.add_argument(
        "json2_path", type=str, help="The path to the second JSON file."
    )
    args = parser.parse_args()

    json1 = load_json(args.json1_path)
    json2 = load_json(args.json2_path)

    merged_json = deep_merge(json1, json2)

    with open("merged_config.json", "w") as file:
        json.dump(merged_json, file, indent=4)


if __name__ == "__main__":
    main()
