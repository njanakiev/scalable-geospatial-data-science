import numpy as np
import pandas as pd
import pyarrow as pa
import dask.dataframe as dd
from dask.distributed import Client
from urllib.parse import urlparse

HDFS_HOME = "hdfs://node-master:54310/user/hadoop"


if __name__ == '__main__':
    numeric_columns = {
        "event_code":      np.int64,
        "event_base_code": np.int64,
        "event_root_code": np.int64,
        "lat":             np.float64,
        "lon":             np.float64,
        "geo_type":        np.int64
    }

    columns_name_mapping = {
        'GLOBALEVENTID':         'event_id',
        'DATEADDED':             'date',
        'SQLDATE':               'event_date', 
        'EventCode':             'event_code', 
        'EventBaseCode':         'event_base_code' ,
        'EventRootCode':         'event_root_code', 
        'ActionGeo_Lat':         'lat', 
        'ActionGeo_Long':        'lon',
        'ActionGeo_Type':        'geo_type',
        'ActionGeo_CountryCode': 'country_code',
        'ActionGeo_ADM1Code':    'adm1_code',
        'SOURCEURL':             'source_url'
    }
    
    schema = pa.schema([
        pa.field('event_id',        pa.string()),
        pa.field('date',            pa.date32()),
        pa.field('event_date',      pa.date32()),
        pa.field('event_code',      pa.int64()),
        pa.field('event_base_code', pa.int64()),
        pa.field('event_root_code', pa.int64()),
        pa.field('lat',             pa.float64()),
        pa.field('lon',             pa.float64()),
        pa.field('geo_type',        pa.int64()),
        pa.field('country_code',    pa.string()),
        pa.field('adm1_code',       pa.string()),
        pa.field('source_url',      pa.string()),
        pa.field('netloc',          pa.string())
    ])
    
    client = Client(memory_limit='6GB', processes=True)
    print("Dashboard Link", client.dashboard_link)
    
    #src_filepath = "data/raw/*.csv"
    src_filepath = "data/raw/2019*.csv"
    #dst_filepath = "processed_data/gdelt_500MB.snappy.parq"
    dst_filepath = "processed_data/gdelt_2019_500MB.snappy.parq"
    #dst_filepath = HDFS_HOME + "/gdelt_2019_500MB.snappy.parq"
    
    df_headers = pd.read_excel('data/CSV.header.fieldids.xlsx')
    columns = df_headers.columns.values
    
    df = dd.read_csv(src_filepath, names=columns, dtype='str', delimiter='\t')
    columns_subset = list(columns_name_mapping.keys())
    df = df[columns_subset]
    df = df.rename(columns=columns_name_mapping)
    
    for col, dtype in numeric_columns.items():
        df[col] = dd.to_numeric(df[col], errors='coerce')
    
    df = df.dropna(subset=[
        'event_code',
        'event_base_code',
        'event_root_code',
        'lat',
        'lon',
        'geo_type'
    ])
    
    for col, dtype in numeric_columns.items():
        if not df[col].dtype == dtype:
             df[col] = df[col].astype(dtype)
    
    df["date"] = dd.to_datetime(
        df["date"], errors='coerce', format="%Y%m%d")
    df["event_date"] = dd.to_datetime(
        df["event_date"], errors='coerce', format="%Y%m%d")
    
    df['netloc'] = df['source_url'].apply(
        lambda url: urlparse(url).netloc if not pd.isna(url) else None,
        meta=('source_url', 'str'))
    
    # Filter wrong dates after 2013
    mask = ((df["date"].dt.year - df["event_date"].dt.year).abs() < 5) | \
            (df["date"].dt.year < 2014)
    df = df[mask]
    
    #df = df.set_index('event_id', sorted=True)
    
    # Repartition dataset
    print(f"Number of partitions: {df.npartitions}")
    #df = df.repartition(npartitions=1)
    df = df.repartition(partition_size="500MB")
    
    print(f"Save to {dst_filepath}")
    df.to_parquet(dst_filepath,
                  engine='pyarrow',
                  schema=schema,
                  compression='snappy')

    
    
# Full 500MB
# real    218m44.256s
# user    651m22.014s
# sys     47m7.481s

# HDFS 2019 500MB (568 partitions)
# real    20m21.166s
# user    60m12.809s
# sys     5m9.280s

# 2019 500MB (568 partitions)
# real    19m52.539s
# user    60m17.722s
# sys     4m53.468s

# OLD

# unsorted filtered dates
# 4925 partitions 
#
# real    142m24.507s
# user    345m47.006s
# sys     23m33.074s

# unsorted filtered dates int columns
# real    130m3.256s
# user    353m3.205s
# sys     25m35.665s

# unsorted filtered dates 100MB partitions
# 3744 partitions
#
# real    246m34.456s
# user    695m41.250s
# sys     44m38.978s

# unsorted filtered dates 500MB partitions
# 569 partitions
#
# real    302m38.417s
# user    692m3.138s
# sys     46m23.634s
