import csv
import time

def stream_csv_to_json(csv_path, delimiter=","):
  with open(csv_path, newline='') as f:
    reader = csv.DictReader(f, delimiter=delimiter)
    for row in reader:
        #print(row)
        yield row

if __name__ == "__main__":
  try:
    for item in stream_csv_to_json('file.csv', delimiter=';'):
        time.sleep(1)
        print(item)
  except Exception as e:
    print(e)