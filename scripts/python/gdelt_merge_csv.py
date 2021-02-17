import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client


if __name__ == '__main__':
    client = Client(memory_limit='14GB', processes=True)
    print("Dashboard Link", client.dashboard_link)
    
    #src_filepath = "data/raw_filtered/2019*.export.csv"
    src_filepath = "data/raw_filtered/2020*.export.csv"
    #src_filepath = "data/raw_filtered/*.export.csv"
    #dst_filepath = "processed_data/2019_filtered.csv"
    dst_filepath = "processed_data/2020_filtered.csv"
    
    df = dd.read_csv(src_filepath)
    
    # https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.to_csv
    df.to_csv(dst_filepath, 
              index=False, 
              single_file=True)
    
# 2020 csv
# real    6m4.852s
# user    5m28.882s
# sys     0m39.676s

# 2019 csv
#real    15m54.717s
#user    13m17.211s
#sys     1m22.761s
