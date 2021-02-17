import os
import time
import pandas as pd
import sqlalchemy
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import Point
from sqlalchemy.engine.url import URL
from dotenv import load_dotenv

load_dotenv()


def convert_geometry(lon, lat):
    if pd.isna(lon) or pd.isna(lat):
        return None
    else:
        return WKTElement(Point(lon, lat).wkt, srid=4326)


def load_to_postgis(filepath, connection_uri, table_name, chunksize=10**5):
    engine = sqlalchemy.create_engine(connection_uri)

    with engine.connect() as connection:
        connection.execute(f'DROP TABLE IF EXISTS {table_name}')
    
    for i, df_chunk in enumerate(pd.read_csv(filepath, chunksize=chunksize)):
        start_time = time.time()
        df_chunk['geometry'] = df_chunk.apply(
            lambda row: convert_geometry(row['ActionGeo_Long'], 
                                         row['ActionGeo_Lat']), axis=1)
        df_chunk.to_sql(table_name, 
                        engine, 
                        if_exists='append', 
                        index=False,
                        dtype={'geometry': Geometry('POINT', srid=4326)})
    
        print('Chunk', i, 'duration: {:.4f}'.format(time.time() - start_time))


if __name__ == '__main__':
    filepath = "processed_data/2020.csv"
    table_name = "gdelt_2020"    
    credentials = {
        'drivername': 'postgres',
        'host':       os.environ['POSTGRES_HOST'],
        'port':       os.environ['POSTGRES_PORT'],
        'username':   os.environ['POSTGRES_USER'],
        'password':   os.environ['POSTGRES_PASS'],
        'database':   os.environ['POSTGRES_DB']
    }
    
    connection_uri = str(URL(**credentials))
    load_to_postgis(filepath, connection_uri, table_name, chunksize=10**5)
