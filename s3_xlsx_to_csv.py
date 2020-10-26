import boto3
import pandas as pd
from io import StringIO
import csv
import time

def xlsx_to_csv(years):
    """Convert xlsx file in aws s3 bucket to pipe delimited .dat files after applying minor data transformations to prep the data for redshift staging"""
    
    start_time = time.time()
    
    s3_resource = boto3.resource('s3')
    s3 = boto3.client('s3')
    for year in years:
        file = 'all_data_M_{}'.format(year)
        obj = s3.get_object(Bucket='dataeng-capstone-1', Key='all_data_M_{}.xlsx'.format(year))
        df = pd.read_excel(obj['Body'].read())
        df.insert(0, 'FY_YEAR', int('{}'.format(year)))             
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, sep="|",index=False,quoting=csv.QUOTE_NONNUMERIC)
        s3_resource.Object('dataeng-capstone-1', 'clean/all_data_M_{}.dat'.format(year)).put(Body=csv_buffer.getvalue())
        print('converted ',file,' from .xlsx to .dat')
    
    
    file =  '2017_NAICS_Descriptions' 
    obj = s3.get_object(Bucket='dataeng-capstone-1', Key='2017_NAICS_Descriptions.xlsx')
    df = pd.read_excel(obj['Body'].read())
    df = df[['Code','Title']]    

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, sep="|",index=False)
    s3_resource.Object('dataeng-capstone-1', 'clean/all_naics_codes.dat').put(Body=csv_buffer.getvalue())
    print('converted ',file,' from .xlsx to .dat')

    end_time = time.time()
    
    runtime = end_time - start_time
    
    print('\n')
    print('runtime: ',runtime)
    print('\n')
    dataend_bucket = s3_resource.Bucket('dataeng-capstone-1')

    print('List files in clean bucket: ')
    for objct in dataend_bucket.objects.filter(Delimiter='/',Prefix='clean/all'):
        print(objct.key)
    
    print('\n')

def main():
    year_list = ['2017','2018']
    
    xlsx_to_csv(year_list)
    
if __name__ == "__main__":
    main()
