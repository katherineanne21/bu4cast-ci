# read_avro.py
from fastavro import reader
import pandas as pd

def read_avro_file(file_path):
    with open(file_path, 'rb') as f:
        avro_reader = reader(f)
        records = list(avro_reader)
    return pd.DataFrame(records)
