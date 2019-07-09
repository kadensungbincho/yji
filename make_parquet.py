import sys
import argparse
import pandas as pd

parser = argparse.ArgumentParser(description='make parquet file from csv')
parser.add_argument('-l','--list', nargs='+', help='<Required> Set flag', required=True)


for fp in sys.argv[1:]:
    parquet_filename = '.'.join([*fp.split('.')[:-1], 'parquet']) 
    pd.read_csv(fp, dtype=str).to_parquet(parquet_filename)