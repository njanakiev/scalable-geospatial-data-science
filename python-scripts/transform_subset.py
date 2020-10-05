import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client


def transform_csv(input_filepath, output_filepath):
    # real    9m39.098s
    # user    14m27.259s
    # sys     6m36.536s
    
    df = dd.read_csv(input_filepath, dtype='str')
    df = df[[
        'GLOBALEVENTID',
        'SQLDATE', 
        'EventCode', 
        'EventBaseCode' ,
        'EventRootCode', 
        'ActionGeo_Lat', 
        'ActionGeo_Long', 
        'SOURCEURL'
    ]]
    
    df["SQLDATE"] = dd.to_datetime(df["SQLDATE"], errors='coerce', format="%Y-%m-%d")
    df['ActionGeo_Lat'] = dd.to_numeric(df['ActionGeo_Lat'], errors='coerce')
    df['ActionGeo_Long'] = dd.to_numeric(df['ActionGeo_Long'], errors='coerce')
    
    df = df.rename(columns={
        'GLOBALEVENTID':  'event_id',
        'SQLDATE':        'event_date', 
        'EventCode':      'event_code', 
        'EventBaseCode':  'event_base_code' ,
        'EventRootCode':  'event_root_code', 
        'ActionGeo_Lat':  'lat', 
        'ActionGeo_Long': 'lon', 
        'SOURCEURL':      'source_url'
    })
    
    df.to_csv(output_filepath, 
              index=False, 
              single_file=True)    

    
def transform_parquet(input_filepath, output_filepath):
    # real    0m27.718s
    # user    1m26.584s
    # sys     0m16.561s
    
    df = dd.read_csv(input_filepath, parse_dates=['event_date'], dtype={
        'event_code': 'str',
        'event_base_code': 'str',
        'event_root_code': 'str',
        'lat': 'float', 
        'lon': 'float'
    })
    print(df.dtypes)
    
    df.to_parquet(output_filepath,
                  engine='pyarrow',
                  compression='snappy')

    
if __name__ == '__main__':
    client = Client(memory_limit='14GB', processes=True)
    print("Dashboard Link", client.dashboard_link)
    
    #input_filepath = "processed_data/2020.csv"
    input_filepath = "processed_data/2020_test.csv"
    #output_filepath = "processed_data/2020_test.csv"
    output_filepath = "processed_data/2020_test.snappy.parq"
    
    #transform_csv(input_filepath, output_filepath)
    transform_parquet(input_filepath, output_filepath)
