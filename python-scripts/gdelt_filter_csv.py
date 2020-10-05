import os
import time
import pandas as pd

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


def filter_csv(src_filepath, columns):
    df = pd.read_csv(src_filepath, names=columns, delimiter='\t')
    df = df[[
        'GLOBALEVENTID',
        'SQLDATE', 
        'DATEADDED',
        'EventCode', 
        'EventBaseCode' ,
        'EventRootCode', 
        'ActionGeo_Lat', 
        'ActionGeo_Long', 
        'SOURCEURL'
    ]]
    df = df.rename(columns={
        'GLOBALEVENTID':  'event_id',
        'DATEADDED':      'date',
        'SQLDATE':        'event_date', 
        'EventCode':      'event_code', 
        'EventBaseCode':  'event_base_code' ,
        'EventRootCode':  'event_root_code', 
        'ActionGeo_Lat':  'lat', 
        'ActionGeo_Long': 'lon', 
        'SOURCEURL':      'source_url'
    })
    
    numeric_columns = {
        'event_id': 'int64', 
        'event_code': 'int64', 
        'event_base_code': 'int64', 
        'event_root_code': 'int64', 
        'lat': 'float64', 
        'lon': 'float64'
    }
    
    df = df.dropna()
    df["date"] = pd.to_datetime(
        df["date"], errors='coerce', format="%Y%m%d")
    df["event_date"] = pd.to_datetime(
        df["event_date"], errors='coerce', format="%Y%m%d")
    
    for col, dtype in numeric_columns.items():
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df = df.dropna()
    
    for col, dtype in numeric_columns.items():
        if not df[col].dtype == dtype:
             df[col] = df[col].astype(dtype)
    
    return df

    
if __name__ == '__main__':
    year_str = '20'
    
    df_headers = pd.read_excel('data/CSV.header.fieldids.xlsx')
    columns = df_headers.columns.values
    
    src_folderpath = 'data/raw'
    dst_folderpath = 'data/raw_filtered'
    
    log_items = []
    filenames = [f for f in os.listdir(src_folderpath) 
                 if f.endswith('csv') and f.startswith(year_str)]
    sum_duration = 0
    num_files = len(filenames)
    
    for i, filename in enumerate(filenames):
        src_filepath = os.path.join(src_folderpath, filename)
        dst_filepath = os.path.join(dst_folderpath, filename)

        print(f"{src_filepath}")
        num_rows = None 
        duration = None
        num_wrong_dates = None
        num_wrong_event_dates = None
        try:
            s = time.time()
            df = filter_csv(src_filepath, columns)
            num_rows = df.shape[0]
            num_wrong_dates       = (df['date'].dt.year < 2000).sum()
            num_wrong_event_dates = (df['event_date'].dt.year < 2000).sum()

            df.to_csv(dst_filepath, index=False)

            duration = time.time() - s
            sum_duration += duration
            avg_duration = sum_duration / (i + 1)
            est_duration = avg_duration * num_files
            
            print(f"{i} / {num_files}, " \
                  f"avg. duration: {round(avg_duration, 2)}, " \
                  f"est. duration: {round(est_duration, 2)}")
            print(f"Duration {round(duration, 2)} s for {num_rows} rows, " \
                  f"num_wrong_dates: {num_wrong_dates}, " \
                  f"num_wrong_event_dates: {num_wrong_event_dates}")
        except Exception as e:
            print("EXCEPTION", e)

        log_items.append({
            'src': src_filepath,
            'dst': dst_filepath,
            'duration': duration,
            'num_rows': num_rows,
            'num_wrong_dates': num_wrong_dates,
            'num_wrong_event_dates': num_wrong_event_dates
        })
            
    
    df = pd.DataFrame(log_items)
    df.to_csv(f"data/filtered_log_{year_str}.csv", index=False)
