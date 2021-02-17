import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client


DTYPES = {
    "event_id":        str,
    "date":            str,
    "event_date":      str,
    "event_code":      np.int64,
    "event_base_code": np.int64,
    "event_root_code": np.int64,
    "lat":             np.float64,
    "lon":             np.float64,
    "geo_type":        np.int64,
    "country_code":    str,
    "adm1_code":       str,
    "source_url":      str,
    "netloc":          str
}


if __name__ == '__main__':
    client = Client(memory_limit='14GB', processes=True)
    print("Dashboard Link", client.dashboard_link)
    
    #src_filepath = "data/raw_filtered/2019*.export.csv"
    #src_filepath = "data/raw_filtered/2020*.export.csv"
    src_filepath = "data/raw_filtered/*.export.csv"
    #dst_filepath = "processed_data/2019_filtered.snappy.parq"
    #dst_filepath = "processed_data/2020_filtered.snappy.parq"
    dst_filepath = "processed_data/gdelt_filtered.snappy.parq"
    
    df = dd.read_csv(src_filepath, 
                     dtype=DTYPES, 
                     parse_dates=['date', 'event_date'])
    
    df = df.set_index('date', sorted=True)
    df.to_parquet(dst_filepath,
                  engine='pyarrow',
                  compression='snappy')

# 2020 parquet
# real    1m49.834s
# user    1m47.804s
# sys     0m23.030s

# 2019 parquet
# real    4m47.002s
# user    3m59.711s
# sys     0m47.536s

# Full parquet
# real    150m2.354s
# user    119m37.066s
# sys     13m59.735s

# time hdfs dfs -put gdelt.snappy.parq/ gdelt_parquet
# real    137m3.985s
# user    2m41.709s
# sys     3m25.887s

# time hdfs dfs -put processed_data/gdelt_filtered.snappy.parq/ gdelt_parquet
# real    58m15.061s
# user    1m23.266s
# sys     1m54.480s
