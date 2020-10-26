import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

stg_raw_h1b_disclosures_drop   =     "DROP TABLE IF EXISTS stg_raw_h1b_disclosures"
stg_clean_h1b_disclosures_drop =     "DROP TABLE IF EXISTS stg_clean_h1b_disclosures"
stg_raw_oes_drop               =     "DROP TABLE IF EXISTS stg_raw_oes"
stg_clean_oes_drop             =     "DROP TABLE IF EXISTS stg_clean_oes"
employer_dim_drop              =     "DROP TABLE IF EXISTS employer_dim"
oes_by_state_dim_drop          =     "DROP TABLE IF EXISTS oes_by_state_dim"
h1b_worker_fact_drop           =     "DROP TABLE IF EXISTS h1b_worker_fact"
lkp_state_codes_drop           =     "DROP TABLE IF EXISTS lkp_state_codes"
lkp_naics_codes_drop           =     "DROP TABLE IF EXISTS lkp_naics_codes"

# TRUNCATE TABLES

stg_raw_h1b_disclosures_trunc   =     "TRUNCATE stg_raw_h1b_disclosures"
stg_clean_h1b_disclosures_trunc =     "TRUNCATE stg_clean_h1b_disclosures"
stg_raw_oes_trunc               =     "TRUNCATE stg_raw_oes"
stg_clean_oes_trunc             =     "TRUNCATE stg_clean_oes"
employer_dim_trunc              =     "TRUNCATE employer_dim"
oes_by_state_dim_trunc          =     "TRUNCATE oes_by_state_dim"
h1b_worker_fact_trunc           =     "TRUNCATE h1b_worker_fact"
lkp_state_codes_trunc           =     "TRUNCATE lkp_state_codes"
lkp_naics_codes_trunc           =     "TRUNCATE lkp_naics_codes"

# CREATE STAGE TABLES (RAW AND CLEAN)

stg_raw_h1b_disclosures_create= ("""
CREATE TABLE IF NOT EXISTS stg_raw_h1b_disclosures
(
FY_YEAR varchar not null distkey,
CASE_NUMBER varchar,
CASE_STATUS varchar,
CASE_SUBMITTED varchar,
DECISION_DATE varchar,
VISA_CLASS varchar,
EMPLOYMENT_START_DATE varchar,
EMPLOYMENT_END_DATE varchar,
EMPLOYER_NAME varchar,
EMPLOYER_BUSINESS_DBA varchar,
EMPLOYER_ADDRESS varchar,
EMPLOYER_CITY varchar,
EMPLOYER_STATE varchar,
EMPLOYER_POSTAL_CODE varchar,
EMPLOYER_COUNTRY varchar,
EMPLOYER_PROVINCE varchar,
EMPLOYER_PHONE varchar,
EMPLOYER_PHONE_EXT varchar,
AGENT_REPRESENTING_EMPLOYER varchar,
AGENT_ATTORNEY_NAME varchar,
AGENT_ATTORNEY_CITY varchar,
AGENT_ATTORNEY_STATE varchar,
JOB_TITLE varchar,
SOC_CODE varchar,
SOC_NAME varchar,
NAICS_CODE varchar,
TOTAL_WORKERS numeric,
NEW_EMPLOYMENT numeric,
CONTINUED_EMPLOYMENT numeric,
CHANGE_PREVIOUS_EMPLOYMENT numeric,
NEW_CONCURRENT_EMP numeric,
CHANGE_EMPLOYER numeric,
AMENDED_PETITION numeric,
FULL_TIME_POSITION varchar,
PREVAILING_WAGE decimal(18,2),
PW_UNIT_OF_PAY varchar,
PW_WAGE_LEVEL varchar,
PW_SOURCE varchar,
PW_SOURCE_YEAR varchar,
PW_SOURCE_OTHER varchar,
WAGE_RATE_OF_PAY_FROM decimal(18,2),
WAGE_RATE_OF_PAY_TO decimal(18,2),
WAGE_UNIT_OF_PAY varchar,
H1B_DEPENDENT varchar,
WILLFUL_VIOLATOR varchar,
SUPPORT_H1B varchar,
LABOR_CON_AGREE varchar,
PUBLIC_DISCLOSURE_LOCATION varchar,
WORKSITE_CITY varchar,
WORKSITE_COUNTY varchar,
WORKSITE_STATE varchar,
WORKSITE_POSTAL_CODE varchar,
ORIGINAL_CERT_DATE varchar
)
""")

stg_clean_h1b_disclosures_create= ("""
CREATE TABLE IF NOT EXISTS stg_clean_h1b_disclosures
(
FY_YEAR int not null distkey,
CASE_NUMBER varchar,
CASE_STATUS varchar,
CASE_SUBMITTED date,
DECISION_DATE date,
VISA_CLASS varchar,
EMPLOYMENT_START_DATE date,
EMPLOYMENT_END_DATE date,
EMPLOYER_NAME varchar,
EMPLOYER_BUSINESS_DBA varchar,
EMPLOYER_ADDRESS varchar,
EMPLOYER_CITY varchar,
EMPLOYER_STATE varchar,
EMPLOYER_POSTAL_CODE varchar,
EMPLOYER_COUNTRY varchar,
EMPLOYER_PROVINCE varchar,
EMPLOYER_PHONE varchar,
EMPLOYER_PHONE_EXT varchar,
AGENT_REPRESENTING_EMPLOYER varchar,
AGENT_ATTORNEY_NAME varchar,
AGENT_ATTORNEY_CITY varchar,
AGENT_ATTORNEY_STATE varchar,
JOB_TITLE varchar,
SOC_CODE varchar,
SOC_NAME varchar,
NAICS_CODE varchar,
TOTAL_WORKERS numeric,
NEW_EMPLOYMENT numeric,
CONTINUED_EMPLOYMENT numeric,
CHANGE_PREVIOUS_EMPLOYMENT numeric,
NEW_CONCURRENT_EMP numeric,
CHANGE_EMPLOYER numeric,
AMENDED_PETITION numeric,
FULL_TIME_POSITION varchar,
PREVAILING_WAGE decimal(18,2),
PW_UNIT_OF_PAY varchar,
PW_WAGE_LEVEL varchar,
PW_SOURCE varchar,
PW_SOURCE_YEAR numeric,
PW_SOURCE_OTHER varchar,
WAGE_RATE_OF_PAY_FROM decimal(18,2),
WAGE_RATE_OF_PAY_TO decimal(18,2),
WAGE_UNIT_OF_PAY varchar,
H1B_DEPENDENT varchar,
WILLFUL_VIOLATOR varchar,
SUPPORT_H1B varchar,
LABOR_CON_AGREE varchar,
PUBLIC_DISCLOSURE_LOCATION varchar,
WORKSITE_CITY varchar,
WORKSITE_COUNTY varchar,
WORKSITE_STATE varchar,
WORKSITE_POSTAL_CODE varchar,
ORIGINAL_CERT_DATE date,
etl_load_dt date
)
""")

stg_raw_oes_create= ("""
CREATE TABLE IF NOT EXISTS stg_raw_oes
(
fy_year         int not null distkey,
area            int,
area_title      varchar,
area_type       int,
naics           varchar,
naics_title     varchar,
i_group         varchar,
own_code        int,
occ_code        varchar,
occ_title       varchar,
o_group         varchar,
tot_emp         varchar,
emp_prse        varchar,
jobs_1000       varchar,
loc_quotient    varchar,
pct_total       varchar,
h_mean          varchar,
a_mean          varchar,
mean_prse       varchar,
h_pct10         varchar,
h_pct25         varchar,
h_median        varchar,
h_pct75         varchar,
h_pct90         varchar,
a_pct10         varchar,
a_pct25         varchar,
a_median        varchar,
a_pct75         varchar,
a_pct90         varchar,
annual          varchar,
hourly          varchar
)
""")

stg_clean_oes_create= ("""
CREATE TABLE IF NOT EXISTS stg_clean_oes
(
fy_year         int not null distkey,
area            int,
area_title      varchar,
area_type       int,
naics           varchar,
naics_title     varchar,
i_group         varchar,
own_code        int,
occ_code        varchar,
occ_title       varchar,
o_group         varchar,
tot_emp         decimal(14,2),
emp_prse        decimal(14,2),
jobs_1000       decimal(14,2),
loc_quotient    decimal(14,2),
pct_total       decimal(14,2),
h_mean          decimal(14,2),
a_mean          decimal(14,2),
mean_prse       decimal(14,2),
h_pct10         decimal(14,2),
h_pct25         decimal(14,2),
h_median        decimal(14,2),
h_pct75         decimal(14,2),
h_pct90         decimal(14,2),
a_pct10         decimal(14,2),
a_pct25         decimal(14,2),
a_median        decimal(14,2),
a_pct75         decimal(14,2),
a_pct90         decimal(14,2),
annual          decimal(14,2),
hourly          decimal(14,2),
etl_load_dt     date
)
""")

lkp_state_codes_create = """
CREATE TABLE IF NOT EXISTS lkp_state_codes
(
 state     varchar
,abbrev    varchar
,code      varchar
)
diststyle all
"""

lkp_naics_codes_create = """
CREATE TABLE IF NOT EXISTS lkp_naics_codes
(
 code        varchar
,title       varchar
)
diststyle all
"""




# CREATE DIMENSION TABLES
# Dimension tables are created with diststyle all as they are small and will speed up performance since they will always be on the same slice as the fact

employer_dim_create = ("""CREATE TABLE IF NOT EXISTS employer_dim
(
 EMPLOYER_DIM_ID             INT IDENTITY(1,1) 
,PERIOD_YEAR_ID              INT              not null distkey
,EMPLOYER_NAME               VARCHAR
,EMPLOYER_BUSINESS_DBA       VARCHAR
,EMPLOYER_ADDRESS            VARCHAR
,EMPLOYER_CITY               VARCHAR
,EMPLOYER_STATE              VARCHAR
,EMPLOYER_POSTAL_CODE        VARCHAR
,EMPLOYER_COUNTRY            VARCHAR
,EMPLOYER_PROVINCE           VARCHAR
,EMPLOYER_PHONE              VARCHAR
,EMPLOYER_PHONE_EXT          VARCHAR
,TOTAL_WORKERS               VARCHAR
,H1B_DEPENDENT               VARCHAR
,WILLFUL_VIOLATOR            VARCHAR
,ETL_LOAD_DT                 TIMESTAMP
,ETL_UPDATE_DT               TIMESTAMP
,primary key(employer_dim_id)
)
""")

oes_by_state_dim = ("""CREATE TABLE IF NOT EXISTS oes_by_state_dim
(
 OES_STATE_DIM_ID             INT IDENTITY(1,1)         
,PERIOD_YEAR_ID               INT                not null distkey      
,STATE_NAME                   VARCHAR            NOT NULL
,STATE_CODE                   VARCHAR            NOT NULL
,SOC_CODE                     VARCHAR            NOT NULL   
,SOC_NAME                     VARCHAR            NOT NULL
,total_EMP_CNT                DECIMAL(14,2)
,jobs_per_1000_in_AREA        DECIMAL(14,2)
,loc_quotient                 DECIMAL(14,2)
,hourly_wage_MEAN             DECIMAL(14,2)
,ANNUAL_WAGE_MEAN             DECIMAL(14,2)
,HOURLY_WAGE_PCT10            DECIMAL(14,2)
,HOURLY_WAGE_pct25            DECIMAL(14,2)
,HOURLY_WAGE_MEDIAN           DECIMAL(14,2)
,HOURLY_WAGE_pct75            DECIMAL(14,2)
,HOURLY_WAGE_pct90            DECIMAL(14,2)
,ANNUAL_WAGE_pct10            DECIMAL(14,2)
,ANNUAL_WAGE_pct25            DECIMAL(14,2)
,ANNUAL_WAGE_median           DECIMAL(14,2)
,ANNUAL_WAGE_pct75            DECIMAL(14,2)
,ANNUAL_WAGE_pct90            DECIMAL(14,2)
,annual_ONLY                  VARCHAR      
,hourly_ONLY                  VARCHAR
,ETL_LOAD_DT                  TIMESTAMP
,ETL_UPDATE_DT                TIMESTAMP
,primary key(OES_STATE_DIM_ID )
)
""")

#CREATE FACT TABLE

h1b_worker_fact_create = ("""CREATE TABLE IF NOT EXISTS h1b_worker_fact
(
 FY_YEAR                      INT        not null distkey
,CASE_NUMBER                  VARCHAR
,OES_STATE_DIM_ID             INT
,EMPLOYER_DIM_ID              INT  
,CASE_STATUS                  VARCHAR
,CASE_SUBMITTED               DATE  
,DECISION_DATE                DATE 
,VISA_CLASS                   VARCHAR
,EMPLOYMENT_START_DATE        DATE  
,EMPLOYMENT_END_DATE          DATE  
,EMPLOYER_NAME                VARCHAR
,JOB_TITLE                    VARCHAR   
,SOC_CODE                     VARCHAR
,SOC_NAME                     VARCHAR
,NAICS_CD                     VARCHAR
,NAICS_NAME                   VARCHAR
,FULL_TIME_POSITION           VARCHAR
,PREVAILING_WAGE              DECIMAL(18,2)
,PW_UNIT_OF_PAY               VARCHAR
,PW_WAGE_LEVEL                VARCHAR
,WAGE_RATE_OF_PAY_FROM        DECIMAL(18,2)
,WAGE_RATE_OF_PAY_TO          DECIMAL(18,2)
,WAGE_UNIT_OF_PAY             VARCHAR
,WORKSITE_CITY                VARCHAR
,WORKSITE_COUNTY              VARCHAR
,WORKSITE_STATE               VARCHAR
,WORKSITE_POSTAL_CODE         VARCHAR
,ETL_LOAD_DT                  TIMESTAMP
,ETL_UPDATE_DT                TIMESTAMP
,primary key(FY_YEAR,CASE_NUMBER)
)
""")



# STAGING RAW TABLES

stg_h1b_disclosures_copy = ("""
copy stg_raw_h1b_disclosures from 's3://dataeng-capstone-1/h1b_disclosure_data_2017_2018.dat'
credentials 'aws_iam_role=arn:aws:iam::910991713532:role/myRedshiftRole'
IGNOREHEADER 1 REMOVEQUOTES;
""")

stg_oes_copy_17 = ("""
copy stg_raw_oes from 's3://dataeng-capstone-1/clean/all_data_M_2017.dat'
credentials 'aws_iam_role=arn:aws:iam::910991713532:role/myRedshiftRole'
IGNOREHEADER 1 REMOVEQUOTES;
""")

stg_oes_copy_18 = ("""
copy stg_raw_oes from 's3://dataeng-capstone-1/clean/all_data_M_2018.dat'
credentials 'aws_iam_role=arn:aws:iam::910991713532:role/myRedshiftRole'
IGNOREHEADER 1 REMOVEQUOTES;
""")

lkp_state_codes_copy = """
copy lkp_state_codes from 's3://dataeng-capstone-1/state_data2.json'
credentials 'aws_iam_role=arn:aws:iam::910991713532:role/myRedshiftRole'
json 'auto ignorecase';
"""

lkp_naics_codes_copy = ("""
copy lkp_naics_codes from 's3://dataeng-capstone-1/clean/all_naics_codes.dat'
credentials 'aws_iam_role=arn:aws:iam::910991713532:role/myRedshiftRole'
IGNOREHEADER 1;
""")

# STAGING CLEAN TABLES
stg_clean_h1b_disclosures_insert = """
insert INTO stg_clean_h1b_disclosures
(
FY_YEAR,
CASE_NUMBER,
CASE_STATUS,
CASE_SUBMITTED,
DECISION_DATE,
VISA_CLASS,
EMPLOYMENT_START_DATE,
EMPLOYMENT_END_DATE,
EMPLOYER_NAME,
EMPLOYER_BUSINESS_DBA,
EMPLOYER_ADDRESS,
EMPLOYER_CITY,
EMPLOYER_STATE,
EMPLOYER_POSTAL_CODE,
EMPLOYER_COUNTRY,
EMPLOYER_PROVINCE,
EMPLOYER_PHONE,
EMPLOYER_PHONE_EXT,
AGENT_REPRESENTING_EMPLOYER,
AGENT_ATTORNEY_NAME,
AGENT_ATTORNEY_CITY,
AGENT_ATTORNEY_STATE,
JOB_TITLE,
SOC_CODE,
SOC_NAME,
NAICS_CODE,
TOTAL_WORKERS,
NEW_EMPLOYMENT,
CONTINUED_EMPLOYMENT,
CHANGE_PREVIOUS_EMPLOYMENT,
NEW_CONCURRENT_EMP,
CHANGE_EMPLOYER,
AMENDED_PETITION,
FULL_TIME_POSITION,
PREVAILING_WAGE,
PW_UNIT_OF_PAY,
PW_WAGE_LEVEL,
PW_SOURCE,
PW_SOURCE_YEAR,
PW_SOURCE_OTHER,
WAGE_RATE_OF_PAY_FROM,
WAGE_RATE_OF_PAY_TO,
WAGE_UNIT_OF_PAY,
H1B_DEPENDENT,
WILLFUL_VIOLATOR,
SUPPORT_H1B,
LABOR_CON_AGREE,
PUBLIC_DISCLOSURE_LOCATION,
WORKSITE_CITY,
WORKSITE_COUNTY,
WORKSITE_STATE,
WORKSITE_POSTAL_CODE,
ORIGINAL_CERT_DATE,
etl_load_dt)
select
 cast(FY_YEAR as int)
,CASE_NUMBER
,CASE_STATUS
,to_date(CASE_SUBMITTED,'yyyy-mm-dd')
,to_date(DECISION_DATE,'yyyy-mm-dd')
,VISA_CLASS
,to_date(EMPLOYMENT_START_DATE,'yyyy-mm-dd')
,to_date(EMPLOYMENT_END_DATE,'yyyy-mm-dd')
,EMPLOYER_NAME
,EMPLOYER_BUSINESS_DBA
,EMPLOYER_ADDRESS
,EMPLOYER_CITY
,EMPLOYER_STATE
,EMPLOYER_POSTAL_CODE
,EMPLOYER_COUNTRY
,EMPLOYER_PROVINCE
,EMPLOYER_PHONE
,EMPLOYER_PHONE_EXT
,AGENT_REPRESENTING_EMPLOYER
,AGENT_ATTORNEY_NAME
,AGENT_ATTORNEY_CITY
,AGENT_ATTORNEY_STATE
,JOB_TITLE
,SOC_CODE
,SOC_NAME
,cast(cast(nullif(regexp_replace(NAICS_CODE,'[^0-9.]+', ''),'') as numeric) as varchar)
,TOTAL_WORKERS
,NEW_EMPLOYMENT
,CONTINUED_EMPLOYMENT
,CHANGE_PREVIOUS_EMPLOYMENT
,NEW_CONCURRENT_EMP
,CHANGE_EMPLOYER
,AMENDED_PETITION
,FULL_TIME_POSITION
,PREVAILING_WAGE
,PW_UNIT_OF_PAY
,PW_WAGE_LEVEL
,PW_SOURCE
,cast(nullif(PW_SOURCE_YEAR,'') as numeric)
,PW_SOURCE_OTHER
,WAGE_RATE_OF_PAY_FROM
,WAGE_RATE_OF_PAY_TO
,WAGE_UNIT_OF_PAY
,H1B_DEPENDENT
,WILLFUL_VIOLATOR
,SUPPORT_H1B
,LABOR_CON_AGREE
,PUBLIC_DISCLOSURE_LOCATION
,WORKSITE_CITY
,WORKSITE_COUNTY
,WORKSITE_STATE
,WORKSITE_POSTAL_CODE
,to_date(ORIGINAL_CERT_DATE,'yyyy-mm-dd')
,current_TIMESTAMP
from
stg_raw_h1b_disclosures
"""

stg_clean_oes_insert = """
insert INTO stg_clean_oes
(
 fy_year     
,area        
,area_title  
,area_type   
,naics       
,naics_title 
,i_group     
,own_code    
,occ_code    
,occ_title   
,o_group     
,tot_emp     
,emp_prse    
,jobs_1000   
,loc_quotient
,pct_total   
,h_mean      
,a_mean      
,mean_prse   
,h_pct10     
,h_pct25     
,h_median    
,h_pct75     
,h_pct90     
,a_pct10     
,a_pct25     
,a_median    
,a_pct75     
,a_pct90     
,annual      
,hourly      
,etl_load_dt 
)
select 
fy_year     
,area        
,area_title  
,area_type   
,naics       
,naics_title 
,i_group     
,own_code    
,occ_code    
,occ_title   
,o_group     
,cast( nullif(regexp_replace(tot_emp,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(emp_prse,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(jobs_1000   ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(loc_quotient,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(pct_total   ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(h_mean      ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(a_mean      ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(mean_prse   ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(h_pct10     ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(h_pct25     ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(h_median    ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(h_pct75     ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(h_pct90     ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(a_pct10     ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(a_pct25     ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(a_median    ,'[^0-9.]+', ''),'') as decimal(14,4)) 
,cast( nullif(regexp_replace(a_pct75     ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(a_pct90     ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(annual      ,'[^0-9.]+', ''),'') as decimal(14,4))
,cast( nullif(regexp_replace(hourly      ,'[^0-9.]+', ''),'') as decimal(14,4))
,current_TIMESTAMP
from stg_raw_oes
"""

#LOADING DIMENSION TABLES

employer_dim_insert = """
insert INTO employer_dim
(
 PERIOD_YEAR_ID        
,EMPLOYER_NAME         
,EMPLOYER_BUSINESS_DBA 
,EMPLOYER_ADDRESS      
,EMPLOYER_CITY         
,EMPLOYER_STATE        
,EMPLOYER_POSTAL_CODE  
,EMPLOYER_COUNTRY      
,EMPLOYER_PROVINCE     
,EMPLOYER_PHONE        
,EMPLOYER_PHONE_EXT    
,TOTAL_WORKERS         
,H1B_DEPENDENT         
,WILLFUL_VIOLATOR      
,ETL_LOAD_DT           
,ETL_UPDATE_DT         
)
SELECT 
 PERIOD_YEAR_ID        
,EMPLOYER_NAME         
,EMPLOYER_BUSINESS_DBA 
,EMPLOYER_ADDRESS      
,EMPLOYER_CITY         
,EMPLOYER_STATE        
,EMPLOYER_POSTAL_CODE  
,EMPLOYER_COUNTRY      
,EMPLOYER_PROVINCE     
,EMPLOYER_PHONE        
,EMPLOYER_PHONE_EXT    
,TOTAL_WORKERS         
,H1B_DEPENDENT         
,WILLFUL_VIOLATOR      
,ETL_LOAD_DT           
,ETL_UPDATE_DT         
FROM 
(
select distinct 
 ROW_NUMBER() OVER (PARTITION BY FY_YEAR,EMPLOYER_NAME ORDER BY EMPLOYER_COUNTRY DESC ) as ROWNUM    
,fy_year                                         as PERIOD_YEAR_ID  
,COALESCE(NULLIF(EMPLOYER_NAME,'')         ,'-') AS EMPLOYER_NAME    
,COALESCE(NULLIF(EMPLOYER_BUSINESS_DBA,'') ,'-') AS EMPLOYER_BUSINESS_DBA
,COALESCE(NULLIF(EMPLOYER_ADDRESS,'')      ,'-') AS EMPLOYER_ADDRESS  
,COALESCE(NULLIF(EMPLOYER_CITY,'')         ,'-') AS EMPLOYER_CITY     
,COALESCE(NULLIF(EMPLOYER_STATE,'-')        ,'-')AS EMPLOYER_STATE    
,COALESCE(NULLIF(EMPLOYER_POSTAL_CODE,'')  ,'-') AS EMPLOYER_POSTAL_CODE 
,COALESCE(NULLIF(EMPLOYER_COUNTRY,'')      ,'-') AS EMPLOYER_COUNTRY  
,COALESCE(NULLIF(EMPLOYER_PROVINCE,'')     ,'-') AS EMPLOYER_PROVINCE 
,COALESCE(NULLIF(EMPLOYER_PHONE,'')        ,'-') AS EMPLOYER_PHONE    
,COALESCE(NULLIF(EMPLOYER_PHONE_EXT,'')    ,'-') AS EMPLOYER_PHONE_EXT  
,sum(TOTAL_WORKERS) OVER (PARTITION BY fy_year,EMPLOYER_NAME ) as total_workers 
,MAX(H1B_DEPENDENT) OVER (PARTITION BY fy_year,EMPLOYER_NAME ) as H1B_DEPENDENT 
,MAX(WILLFUL_VIOLATOR) OVER (PARTITION BY fy_year,EMPLOYER_NAME ) as WILLFUL_VIOLATOR      
,current_timestamp as ETL_LOAD_DT           
,current_timestamp as ETL_UPDATE_DT         
from
stg_clean_h1b_disclosures 
  ) A WHERE ROWNUM = 1
"""

oes_by_state_dim_insert = """
insert INTO oes_by_state_dim
(
 PERIOD_YEAR_ID       
,STATE_NAME           
,STATE_CODE           
,SOC_CODE             
,SOC_NAME             
,total_EMP_CNT        
,jobs_per_1000_in_AREA
,loc_quotient         
,hourly_wage_MEAN     
,ANNUAL_WAGE_MEAN     
,HOURLY_WAGE_PCT10    
,HOURLY_WAGE_pct25    
,HOURLY_WAGE_MEDIAN   
,HOURLY_WAGE_pct75    
,HOURLY_WAGE_pct90    
,ANNUAL_WAGE_pct10    
,ANNUAL_WAGE_pct25    
,ANNUAL_WAGE_median   
,ANNUAL_WAGE_pct75    
,ANNUAL_WAGE_pct90    
,annual_ONLY          
,hourly_ONLY          
,ETL_LOAD_DT          
,ETL_UPDATE_DT           
)
select distinct
 oes.fy_year              
,oes.area_title        
,lkp.code             
,oes.occ_code          
,oes.occ_title         
,oes.tot_emp           
,oes.jobs_1000         
,oes.loc_quotient      
,oes.h_mean            
,oes.a_mean            
,oes.h_pct10           
,oes.h_pct25           
,oes.h_median          
,oes.h_pct75           
,oes.h_pct90           
,oes.a_pct10           
,oes.a_pct25           
,oes.a_median          
,oes.a_pct75           
,oes.a_pct90           
,oes.annual            
,oes.hourly            
,current_timestamp   
,current_timestamp 
from
STG_CLEAN_OES oes
left join lkp_state_codes lkp on upper(oes.area_title) = upper(lkp.state)
WHERE
area_type = '2'and 
o_group = 'detailed'
"""

#Loading Fact Tables

h1b_worker_fact_insert = """
insert INTO h1b_worker_fact
(
 FY_YEAR              
,CASE_NUMBER          
,OES_STATE_DIM_ID     
,EMPLOYER_DIM_ID      
,CASE_STATUS          
,CASE_SUBMITTED       
,DECISION_DATE        
,VISA_CLASS           
,EMPLOYMENT_START_DATE
,EMPLOYMENT_END_DATE  
,EMPLOYER_NAME        
,JOB_TITLE            
,SOC_CODE             
,SOC_NAME             
,NAICS_CD             
,NAICS_NAME           
,FULL_TIME_POSITION   
,PREVAILING_WAGE      
,PW_UNIT_OF_PAY       
,PW_WAGE_LEVEL        
,WAGE_RATE_OF_PAY_FROM
,WAGE_RATE_OF_PAY_TO  
,WAGE_UNIT_OF_PAY     
,WORKSITE_CITY        
,WORKSITE_COUNTY      
,WORKSITE_STATE       
,WORKSITE_POSTAL_CODE 
,ETL_LOAD_DT          
,ETL_UPDATE_DT        
)
select
 h1b.FY_YEAR               
,h1b.CASE_NUMBER           
,coalesce(oes.OES_STATE_DIM_ID,-1) 
,coalesce(emp.EMPLOYER_DIM_ID,-1)       
,h1b.CASE_STATUS           
,h1b.CASE_SUBMITTED        
,h1b.DECISION_DATE         
,h1b.VISA_CLASS            
,h1b.EMPLOYMENT_START_DATE 
,h1b.EMPLOYMENT_END_DATE   
,coalesce(nullif(h1b.EMPLOYER_NAME,''),'-')    
,h1b.JOB_TITLE             
,h1b.SOC_CODE              
,h1b.SOC_NAME              
,h1b.NAICS_CoDe              
,naics.title as NAICS_NAME            
,h1b.FULL_TIME_POSITION    
,h1b.PREVAILING_WAGE      
,h1b.PW_UNIT_OF_PAY        
,h1b.PW_WAGE_LEVEL         
,h1b.WAGE_RATE_OF_PAY_FROM
,h1b.WAGE_RATE_OF_PAY_TO
,h1b.WAGE_UNIT_OF_PAY      
,h1b.WORKSITE_CITY         
,h1b.WORKSITE_COUNTY       
,h1b.WORKSITE_STATE        
,h1b.WORKSITE_POSTAL_CODE 
,current_timestamp
,current_timestamp
from
stg_clean_h1b_disclosures h1b 
left join employer_dim emp on h1b.FY_YEAR = emp.PERIOD_YEAR_ID  and coalesce(nullif(h1b.EMPLOYER_NAME,''),'-') = emp.EMPLOYER_NAME
left join oes_by_state_dim oes on h1b.FY_YEAR = oes.PERIOD_YEAR_ID  and h1b.WORKSITE_STATE  = oes.STATE_CODE and h1b.SOC_CODE = oes.SOC_CODE
left join lkp_naics_codes naics on h1b.NAICS_CoDe = naics.code
"""

# DATA Quality Checks

check_1_sql = """
select count(*) from {}
"""

oes_state_code_check = """
select count(distinct state_code) from oes_by_state_dim
"""



# QUERY LISTS

create_table_queries = [
    stg_raw_h1b_disclosures_create,
    stg_clean_h1b_disclosures_create,
    stg_raw_oes_create,
    stg_clean_oes_create,
    employer_dim_create,
    oes_by_state_dim,
    h1b_worker_fact_create ,
    lkp_state_codes_create,
    lkp_naics_codes_create
]

drop_table_queries   = [
    stg_raw_h1b_disclosures_drop,
    stg_clean_h1b_disclosures_drop,
    stg_raw_oes_drop,
    stg_clean_oes_drop,
    employer_dim_drop,
    oes_by_state_dim_drop,
    h1b_worker_fact_drop ,
    lkp_state_codes_drop,
    lkp_naics_codes_drop
]

stage_raw_tables     = [
    stg_h1b_disclosures_copy,
    stg_oes_copy_17,
    stg_oes_copy_18,
    lkp_state_codes_copy,
    lkp_naics_codes_copy
]

stage_clean_tables   = [
    stg_clean_h1b_disclosures_insert,
    stg_clean_oes_insert
]

load_dim_tables      = [
    employer_dim_insert,
    oes_by_state_dim_insert
]

load_fact_tables      = [
    h1b_worker_fact_insert
]

truncate_raw_tables = [
    stg_raw_h1b_disclosures_trunc,  
    stg_raw_oes_trunc,                      
    lkp_state_codes_trunc,          
    lkp_naics_codes_trunc            
]

truncate_clean_tables = [
    stg_clean_h1b_disclosures_trunc,            
    stg_clean_oes_trunc                       
]

truncate_dim_tables = [          
    employer_dim_trunc,             
    oes_by_state_dim_trunc                 
]

truncate_fact_tables = [       
    h1b_worker_fact_trunc           
]