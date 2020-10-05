import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client


if __name__ == '__main__':
    client = Client(memory_limit='14GB', processes=True)
    print("Dashboard Link", client.dashboard_link)
    
    #src_filepath = "data/raw_filtered/2019*.export.csv"
    #src_filepath = "data/raw_filtered/2020*.export.csv"
    src_filepath = "data/raw_filtered/201*.export.csv"
    #dst_filepath = "processed_data/2019_filtered.csv"
    #dst_filepath = "processed_data/2020_filtered.csv"
    #dst_filepath = "processed_data/2019_filtered.snappy.parq"
    #dst_filepath = "processed_data/2020_filtered.snappy.parq"
    dst_filepath = "processed_data/201X_filtered.snappy.parq"
    
    df = dd.read_csv(src_filepath)
    #df["date"] = dd.to_datetime(
    #    df["date"], errors='coerce', format="%Y-%m-%d")
    #df["event_date"] = dd.to_datetime(
    #    df["event_date"], errors='coerce', format="%Y-%m-%d")
    
    # https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.to_csv
    #df.to_csv(dst_filepath, 
    #          index=False, 
    #          single_file=True)
    
    df = df.set_index('date')
    df.to_parquet(dst_filepath,
                  engine='pyarrow',
                  compression='snappy')

# 2020 csv
# real    6m4.852s
# user    5m28.882s
# sys     0m39.676s

# 2019 csv
#real    15m54.717s
#user    13m17.211s
#sys     1m22.761s

# 2020 parquet
# real    1m49.834s
# user    1m47.804s
# sys     0m23.030s

# 2019 parquet
# real    4m47.002s
# user    3m59.711s
# sys     0m47.536s
