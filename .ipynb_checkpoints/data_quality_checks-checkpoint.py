import configparser
import psycopg2
from sql_queries import check_1_sql, oes_state_code_check
import time
import pandas as pd

def data_quality_raw(cur):
    """Check whether raw tables have more the 0 rows loaed."""
    
    print("Running Data Quality Checks for raw tables...")
    
    raw_tables = {'stg_raw_h1b_disclosures':0,'stg_raw_oes':0,'lkp_state_codes':0,'lkp_naics_codes':0}
    
    for table in raw_tables:
        cur.execute(check_1_sql.format(table))
        result = cur.fetchall()
        raw_tables[table] = result[0][0]
        
    print(raw_tables)
    
    if 0 in raw_tables.values():
        print('raw table data quality check failed')
    else:
        print('raw table data quality check passed')

def data_quality_clean(cur):
    """Check whether clean tables have more than 0 rows loaded"""
    
    print("Running Data Quality Checks for clean tables...")
    clean_tables = {'stg_clean_h1b_disclosures':0,'stg_clean_oes':0}
    
    for table in clean_tables:
        cur.execute(check_1_sql.format(table))
        result = cur.fetchall()
        clean_tables[table] = result[0][0]
        
    print(clean_tables)
        
    if 0 in clean_tables.values():
        print('clean table data quality check failed')
    else:
        print('clean table data quality check passed')

def data_quality_dim(cur):
    """Check whether dimension tables have more than 0 rows loaded and check that there 51 distinct state codes in OES dim table"""
    
    print("Running Data Quality Checks for dim tables...")
    
    dim_tables = {'employer_dim':0,'oes_by_state_dim':0}
    
    for table in dim_tables:
        cur.execute(check_1_sql.format(table))
        result = cur.fetchall()
        dim_tables[table] = result[0][0]
        
    print(dim_tables)
    
    if 0 in dim_tables.values():
        print('General dim table data quality check failed')
    else:
        print('General dim table data quality check passed')
        
    cur.execute(oes_state_code_check)
    result = cur.fetchall()
    state_cnt = result[0][0]
    
    print('distinct state codes in oes_by_state_dim = ',state_cnt)
    
    if state_cnt != 51:
        print('OES dim table state code data quality check failed')
    else:
        print('OES dim table state code data quality check passed')


    

def data_quality_fact(cur):
    """Check whether fact tables have more than 0 rows loaded."""
    
    print("Running Data Quality Checks for fact tables...")
    
    fact_tables = {'h1b_worker_fact':0}
    
    for table in fact_tables:
        cur.execute(check_1_sql.format(table))
        result = cur.fetchall()
        fact_tables[table] = result[0][0]
        
    print(fact_tables)
    
    if 0 in fact_tables.values():
        print('General fact table data quality check failed')
    else:
        print('General fact table data quality check passed')
    

def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    data_quality_raw(cur)
    data_quality_clean(cur)
    data_quality_dim(cur)
    data_quality_fact(cur)
    
    conn.close()
    

if __name__ == "__main__":
    main()
