import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client

# 2020
# real    32m52.778s
# user    45m50.147s
# sys     10m9.458s


def merge(input_filepath, output_filepath):
    df_headers = pd.read_excel('data/CSV.header.fieldids.xlsx')
    columns = df_headers.columns.values
    
    numeric_columns = {
        "Year": "int",
        "FractionDate": "float",
        "Actor1Geo_Type": "int",
        "Actor1Geo_Lat": "float",
        "Actor1Geo_Long": "float",
        "Actor2Geo_Type": "int",
        "Actor2Geo_Lat": "float",
        "Actor2Geo_Long": "float",
        "ActionGeo_Lat": "float",
        "ActionGeo_Long": "float",
        "IsRootEvent": "int",
        "EventCode": "int",
        "EventBaseCode": "int",
        "EventRootCode": "int",
        "QuadClass": "int",
        "GoldsteinScale": "float",
        "NumMentions": "int",
        "NumSources": "int",
        "NumArticles": "int",
        "AvgTone": "float"
    }

    df = dd.read_csv(input_filepath, 
                     names=columns,
                     delimiter='\t',
                     dtype="str")

    for column in numeric_columns:
        df[column] = dd.to_numeric(df[column], errors='coerce')
    
    df["SQLDATE"]   = dd.to_datetime(df["SQLDATE"], errors='coerce', format="%Y%m%d")
    df["DATEADDED"] = dd.to_datetime(df["DATEADDED"], errors='coerce', format="%Y%m%d")

    # https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.to_csv
    df.to_csv(output_filepath, 
              index=False, 
              single_file=True)

    
if __name__ == '__main__':
    client = Client(memory_limit='14GB', processes=True)
    print("Dashboard Link", client.dashboard_link)
    
    #input_filepath = "data/raw/2019*.export.csv"
    input_filepath = "data/raw/2020*.export.csv"
    #input_filepath = "data/*.csv"
    output_filepath = "processed_data/2020.csv"
    
    merge(input_filepath, output_filepath)
