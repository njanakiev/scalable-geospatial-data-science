import os
import time
import pandas as pd
from urllib.parse import urlparse

# 2020
# real    20m17.494s
# user    11m56.839s
# sys     0m39.823s

# 2019
# real    55m9.319s
# user    26m4.964s
# sys     1m21.729s

# 20XX
# real    451m42.280s
# user    220m21.252s
# sys     14m57.931s

# Full
# real    653m22.214s
# user    368m54.816s
# sys     13m25.819s

# time hdfs dfs -put /home/hadoop/sgds/data/raw/*.csv /user/hadoop/gdelt_raw
# real    222m33.449s
# user    8m55.517s
# sys     10m25.576s

def filter_csv(src_filepath, columns):
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
    
    df = pd.read_csv(src_filepath, names=columns, delimiter='\t')
    df = df[columns_name_mapping.keys()]
    df = df.rename(columns=columns_name_mapping)
    
    numeric_columns = {
        'event_id': 'int64', 
        'event_code': 'int64', 
        'event_base_code': 'int64', 
        'event_root_code': 'int64', 
        'lat': 'float64', 
        'lon': 'float64'
    }
    
    for col, dtype in numeric_columns.items():
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df = df.dropna(subset=[
        'event_code',
        'event_base_code',
        'event_root_code',
        'geo_type'
    ])
        
    for col, dtype in numeric_columns.items():
        if not df[col].dtype == dtype:
             df[col] = df[col].astype(dtype)
    
    df["date"] = pd.to_datetime(
        df["date"], errors='coerce', format="%Y%m%d")
    df["event_date"] = pd.to_datetime(
        df["event_date"], errors='coerce', format="%Y%m%d")
    
    df['netloc'] = df['source_url'].apply(
        lambda url: urlparse(url).netloc if not pd.isna(url) else None)
    
    return df


if __name__ == '__main__':
    df_headers = pd.read_excel('data/CSV.header.fieldids.xlsx')
    columns = df_headers.columns.values
    
    src_folderpath = 'data/raw'
    dst_folderpath = 'data/raw_filtered'
    
    log_items = []
    filenames = [f for f in os.listdir(src_folderpath) if f.endswith('csv')]
    sum_duration = 0
    num_files = len(filenames)
    
    for i, filename in enumerate(sorted(filenames)):
        src_filepath = os.path.join(src_folderpath, filename)
        dst_filepath = os.path.join(dst_folderpath, filename)
        year = int(filename[:4])
        
        print(f"{src_filepath}")
        num_rows = None
        duration = None
        num_wrong_dates = None
        try:
            s = time.time()
            df = filter_csv(src_filepath, columns)
            num_wrong_dates = (df['event_date'].dt.year != year).sum()
            
            # filter wrong dates
            mask = (df['event_date'].dt.year - year).abs() < 5
            df = df[mask]

            df.to_csv(dst_filepath, index=False)
            num_rows = df.shape[0]
            
            duration = time.time() - s
            sum_duration += duration
            avg_duration = sum_duration / (i + 1)
            est_duration = avg_duration * num_files
            
            print(f"{i} / {num_files}, " \
                  f"avg. duration: {round(avg_duration, 2)}, " \
                  f"est. duration: {round(est_duration, 2)}")
            print(f"Duration {round(duration, 2)} s for {num_rows} rows, " \
                  f"num_wrong_dates: {num_wrong_dates}")
        except Exception as e:
            print("EXCEPTION", e)

        log_items.append({
            'src': src_filepath,
            'dst': dst_filepath,
            'duration': duration,
            'num_rows': num_rows,
            'num_wrong_dates': num_wrong_dates
        })
    
    df = pd.DataFrame(log_items)
    df.to_csv("data/filtered_stats_full.csv", index=False)
