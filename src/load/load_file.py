import json
import pandas


def dump_to_json_file(data, file_path, mode):
    try:
        with open(file_path, mode=mode) as json_file:
            json.dump(data, json_file, indent=4)
    except Exception as e:
        print(f"Loi khi dump_to_json_file: \n{e}")

def load_json_file(file_path, mode):
    with open(file=file_path, mode=mode) as file:
        data = json.load(file)
    return data

def dataframe_to_csv(data, file_path):
    try:
        data.to_csv(file_path, index=False)
    except Exception as e:
        print(f"Loi khi save df vao file csv: {file_path}\n{e}")

def load_csv_file(file_path):
    return pandas.read_csv(file_path)