import configparser
import psycopg2
from sql_queries import stage_raw_tables, stage_clean_tables, load_dim_tables, load_fact_tables,truncate_raw_tables,truncate_clean_tables,truncate_dim_tables,truncate_fact_tables
from data_quality_checks import data_quality_raw, data_quality_clean, data_quality_dim, data_quality_fact
import time
from s3_xlsx_to_csv import xlsx_to_csv


def load_staging_tables(cur, conn):
    """Truncate redshift stage tables and then loads data from s3"""
    
    for query in truncate_raw_tables:
        print('truncating tables...')
        cur.execute(query)
        conn.commit()
        print('Table truncated.')
        print('\n')
        
    
    for query in stage_raw_tables:
        #print('executing query: ',query)
        print('loading tables...')
        cur.execute(query)
        conn.commit()
        print('Table loaded.')
        print('\n')

def load_clean_tables(cur, conn):
    """Truncate and load redshift clean staging tables from raw staging tables"""
    
    for query in truncate_clean_tables:
        print('truncating tables...')
        cur.execute(query)
        conn.commit()
        print('Table truncated.')
        print('\n')
    
    for query in stage_clean_tables:
        #print('executing query: ',query)
        print('query running...')
        cur.execute(query)
        conn.commit()
        print('Table loaded.')
        print('\n')

def load_dimension_tables(cur, conn):
    """Truncate and load dimension tables from clean staging tables"""
    
    for query in truncate_dim_tables:
        print('truncating tables...')
        cur.execute(query)
        conn.commit()
        print('Table truncated.')
        print('\n')
    
    for query in load_dim_tables:
        #print('executing query: ',query)
        print('query running...')
        cur.execute(query)
        conn.commit()
        print('Table loaded.')
        print('\n')        

def load_fact_table(cur, conn):
    """Truncate and load Fact tables from clean staging tables and dimension tables"""
    
    for query in truncate_fact_tables:
        print('truncating tables...')
        cur.execute(query)
        conn.commit()
        print('Table truncated.')
        print('\n')
    
    for query in load_fact_tables:
        #print('executing query: ',query)
        print('query running...')
        cur.execute(query)
        conn.commit()
        print('Table loaded.')
        print('\n')   


def main():
    """Call all the loads table functions as well as perform data quality checks after each load"""
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    main_start = time.time()
    
    print('==============PREPROCESSING DATA IN AWS S3==============')
    
    #preprocess s3 data 
    
    years = ['2017','2018']
    
    xlsx_to_csv(years)
    
    print('==============STAGING RAW TABLES==============')
    
    stage_raw_start = time.time()
    
    load_staging_tables(cur, conn)
    stage_raw_end = time.time()
    
    stage_raw_runtime = stage_raw_end - stage_raw_start
    
    print('stage raw tables runtime (seconds): ', stage_raw_runtime)
    
    print('\n')
    
    # run data quality checks for raw tables
    data_quality_raw(cur)
    
    print('\n\n')
    
    print('==============STAGING CLEAN TABLES==============')
    
    stage_clean_start = time.time()
    
    load_clean_tables(cur, conn)
    
    stage_clean_end = time.time()
    
    stage_clean_runtime = stage_clean_end - stage_clean_start
    
    print('stage clean tables runtime (seconds): ', stage_clean_runtime)
    
    print('\n')
    
    # run data quality checks for clean tables

    data_quality_clean(cur)
    
    print('\n\n')
    
    print('==============Loading DIMENSION TABLES==============')
    
    dim_start = time.time()
    
    load_dimension_tables(cur, conn)
    
    dim_end = time.time()
    
    dim_runtime = dim_end - dim_start
    
    print('dim tables runtime (seconds): ', dim_runtime)
    
    print('\n')
    
    # run data quality checks for dim tables
    data_quality_dim(cur)
    
    print('\n\n')
          
    print('==============Loading FACT TABLES==============')
    
    fact_start = time.time()
    
    load_fact_table(cur, conn)
    
    fact_end = time.time()
    
    fact_runtime = fact_end - fact_start
    
    print('fact tables runtime (seconds): ', fact_runtime)
          
    print('\n')
    
    # run data quality checks for fact tables
    data_quality_fact(cur)
    
    main_end = time.time()
    
    main_runtime = main_end - main_start
    
    print('\n\n')
    
    print('TOTAL ETL runtime (seconds): ',main_runtime)
   

    conn.close()


if __name__ == "__main__":
    main()