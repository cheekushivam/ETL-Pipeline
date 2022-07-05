# ETL-Pipeline
Simple ETL Pipeline Using Snowflake, AWS, and PySpark.

![ETL Pipeline Snowflake and AWS_jpeg](https://user-images.githubusercontent.com/30614426/177395685-dafe5ed2-cf91-4263-95bf-fa62218f0cfa.jpg)

Prerequisites

1. Snowflake Account, please go with the trial version.
2. AWS Account, please go with the aws trial account.
3. Covid-19 dataset, please use below link to download.
   https://www.kaggle.com/datasets/sudalairajkumar/covid19-in-india 


Components

1. Data Lake - AWS S3
2. Data Warehouse - Snowflake
3. Data Transformation - PySpark
4. Triggering Events - AWS SNS, Snowflake SQS

# AWS Setup with Snowflake Storage Integration

1. In IAM > Roles > Create New Role > AWS Account (Requires External ID)
   Initially proceed with random external id like '00000000'

2. Attach policies with full access to S3, SNS and SQS.

3. Proceed with default actions and create role.

4. Create snowflake Storage Integration Object using following syntax:

create storage integration s3_int
  type = external_stage,
  storage_provider = s3,
  enabled = true,
  storage_aws_role_arn = 'ARN of the new role created',
  storage_allowed_locations = ('Path to S3 Bucket');

5. In S3 > Create Bucket > Provide name and Choose Region and go with the default
   selcted options.
   Note : Region should be same where snowflake account is created.

6. Update path in above query to storage_allowed_locations like below
   Path : s3://{providebucketname}/

7. Run 'DESCRIBE integration S3_INT'.

8. Edit Trust Relationships JSON part - AWS arn and External ID from the output
   running describe command. 

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "Copy STORAGE_AWS_IAM_USER_ARN"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId":  "Copy STORAGE_AWS_EXTERNAL_ID"
                }
            }
        }
    ]
}

9. Update the policy and we are done with the role, bucket and integration setup.

# AWS SNS Connection to Snowflake

1. In Amazon SNS > Topics > Create Topic > Type FIFO > Provide Name > Default
   options

2. Define access policy to Everyone - Not Recommended Only for trial purpose

3. Keep the copy of the JSON preview handy as we will be updating this JSON to
   register events to Snowflake SQS. Finally, click on Create Topic.

4. Now in snowflake run following query and copy statement part of the jSON 
   and paste it in the last to the JSON we copied from JSON preview of SNS
   and update SNS and save. 

Query - select system$get_aws_sns_iam_policy('PUT ARN of the SNS created');

This part of above code output should be pasted to SNS JSON Preview.

{
"Sid":"1","Effect":"Allow","Principal":{"AWS":"XXXXX"},"Action":["sns:Subscribe"],"Resource":["XXXXXXX"]
}

5. Finally, we are done with the setup of setting automatic data flow from S3 to 
   Snowflake by registering events in while putting file in S3 and sending message
   to SQS queue to update data in Snowflake.

# SnowFlake Objects Creation Part - 1

STAGING AND FINAL TABLE OBJECTS IN SNOWFLAKE
--------------------------------------------

1. Create Staging Database to store schemas

Create database ETL_PROJECT_STAGING;

2. Create table to store covid-19 staging data.

create or replace TABLE ETL_PROJECT_STAGING.PUBLIC.COVID_19 (	
SNO NUMBER(38,0) autoincrement,	
REPORTEDDATE VARCHAR(100),	
STATE VARCHAR(50),	
CONFIRMEDINDIANNATIONAL NUMBER(38,0),	
CONFIRMEDFOREIGNNATIONAL NUMBER(38,0),	
CURED NUMBER(38,0),	DEATHS NUMBER(38,0),	
CONFIRMED NUMBER(38,0))
COMMENT='This is covid_19 India Data';

3. Create File Format as we will be having csv file which needs to be loaded in table.

Create file format ETL_PROJECT_STAGING.PUBLIC.COVID_CSV
type = 'csv',
field_delimiter = ',',
record_delimiter = '\n',
skip_header = 1,
field_optionally_enclosed_by = '\042',null_if = ('\\N');

4. Create an external staging object using S3 Bucket location and storage integration object

create or replace stage covid_s3_stage 
storage_integration = S3_INT  
url = 's3://snowflakecoviddata/covid_19/'  
file_format = ETL_PROJECT_STAGING.PUBLIC.COVID_CSV;

5. Create pipe object to ingest data automatically from S3 to table in snowflake
   whenever data gets landed to S3.

create or replace pipe ETL_PROJECT_STAGING.PUBLIC.s3_to_snowflake_covid_19
auto_ingest = true,
aws_sns_topic = 'arn:aws:sns:us-east-2:681515040523:etl_covid'
as
copy into ETL_PROJECT_STAGING.PUBLIC.COVID_19 from @ETL_PROJECT_STAGING.PUBLIC.covid_s3_stagefile_format = ETL_PROJECT_STAGING.PUBLIC.covid_csv;

6. Validate whether all the objects got created successfully.

Show Databases;
Show Schemas;
Show tables;
Show file formats;
Show stages;
Show Pipes;

7. Check snowpipe status using query below. It should be in paused status.

Select system$pipe_status('ETL_PROJECT_STAGING.PUBLIC.s3_to_snowflake_covid_19');

8. Now resume the pipe status using below query

alter pipe ETL_PROJECT_STAGING.PUBLIC.s3_to_snowflake_covid_19 set pipe_execution_paused = true;

9. Create Final Database to store schemas.

Create database ETL_PROJECT_FINAL;

10. Create table to store covid-19 final data.

create or replace TABLE ETL_PROJECT_FINAL.PUBLIC.COVID_19 (
SNO NUMBER(38,0) autoincrement,	
REPORTEDDATE VARCHAR(100),	
STATE VARCHAR(50),	
CONFIRMEDINDIANNATIONAL NUMBER(38,0),	
CONFIRMEDFOREIGNNATIONAL NUMBER(38,0),	
CURED NUMBER(38,0),	DEATHS NUMBER(38,0),	
CONFIRMED NUMBER(38,0))
COMMENT='This is covid_19 India Data';

Let's proceed with the historical data load. This process will be done automatically, we just need to create a folder inside s3 bucket previously created with whatever name you provided. Create 'covid-19' folder and place a initial file with 1000 rows, which we are treating as historical load. Once file is placed, automatically in couple of minutes data will get loaded to staging table.

# Historical Data Load and Verification.

Historical Data Format Sample
-----------------------------

SNO,REPORTEDDATE,STATE,CONFIRMEDINDIANNATIONAL,CONFIRMEDFOREIGNNATIONAL,CURED,DEATHS,CONFIRMED
1,30-01-2020,Kerala,1,0,0,0,1
2,31-01-2020,Kerala,1,0,0,0,1
3,01-02-2020,Kerala,2,0,0,0,2
4,02-02-2020,Kerala,3,0,0,0,3
5,03-02-2020,Kerala,3,0,0,0,3
6,04-02-2020,Kerala,3,0,0,0,3
7,05-02-2020,Kerala,3,0,0,0,3
8,06-02-2020,Kerala,3,0,0,0,3
9,07-02-2020,Kerala,3,0,0,0,3
10,08-02-2020,Kerala,3,0,0,0,3

1. Please verify the data in staging table using below query.

Select * from ETL_PROJECT_STAGING.PUBLIC.COVID_19;

2. Now move this historical data to final table using below query.

Insert into ETL_PROJECT_FINAL.PUBLIC.COVID_19(
SNO,
REPORTEDDATE,	
STATE,	
CONFIRMEDINDIANNATIONAL,	
CONFIRMEDFOREIGNNATIONAL,	
CURED,	
DEATHS,	
CONFIRMED)    
Select * from ETL_PROJECT_STAGING.PUBLIC.COVID_19;

3. Please verify the data in final table using below query.

Select * from ETL_PROJECT_STAGING.PUBLIC.COVID_19;

# SnowFlake Objects Creation Part - 2

STAGING AND FINAL TABLE OBJECTS IN SNOWFLAKE
--------------------------------------------

11. Create Stream to capture data change when any delta data gets loaded into
    staging table.

    Note : Make append_only = true to force system to add or update new records.

Create stream ETL_PROJECT_STAGING.PUBLIC.covid_19_stream on table ETL_PROJECT_STAGING.PUBLIC.COVID_19 append_only = true;

12. Create Task to transfer changes in staging table to final table automatically in the scheduled time i.e. (1 minute). By default it will be in stopped status.

Logic
-----
check fields - Reported Date and State pair

If Reported Date and State pair is not available then new record creation.
If Reported Date and State pair is available then update data.

Use below query to prepare task.

Create or replace task task_covid_19    
WAREHOUSE  = compute_wh    
SCHEDULE  = '1 MINUTE'
WHEN  
SYSTEM$STREAM_HAS_DATA('ETL_PROJECT_STAGING.PUBLIC.covid_19_stream')
As
merge into ETL_PROJECT_FINAL.PUBLIC.COVID_19 cov_final 
using ETL_PROJECT_STAGING.PUBLIC.covid_19_stream cov_stg_stream on cov_stg_stream.REPORTEDDATE = cov_final.REPORTEDDATE 
and 
cov_stg_stream.state = cov_final.state
when matched    
then update set
cov_final.CONFIRMEDINDIANNATIONAL = cov_stg_stream.CONFIRMEDINDIANNATIONAL,
cov_final.CONFIRMEDFOREIGNNATIONAL = cov_stg_stream.CONFIRMEDFOREIGNNATIONAL,
cov_final.CURED = cov_stg_stream.CURED,
cov_final.DEATHS = cov_stg_stream.DEATHS,
cov_final.CONFIRMED = cov_stg_stream.CONFIRMED
when not matched    
then insert (
REPORTEDDATE,	
STATE,	
CONFIRMEDINDIANNATIONAL,	
CONFIRMEDFOREIGNNATIONAL,	
CURED,	
DEATHS,	
CONFIRMED)    
Values(
cov_stg_stream.REPORTEDDATE,        
cov_stg_streamcov_stg_stream.STATE,        cov_stg_stream.CONFIRMEDINDIANNATIONAL,        cov_stg_stream.CONFIRMEDFOREIGNNATIONAL,       
 cov_stg_stream.CURED,        
cov_stg_stream.DEATHS,        
cov_stg_stream.CONFIRMED);

13. Resume the task using below query.

ALTER TASK IF EXISTS task_covid_19 RESUME;

Let's proceed with the delta data load. This process will be done automatically, we just need to place new file inside 'covid-19' folder which would consists of updated row from the loaded records. Please find below sample 2 records updated. Once, this file got loaded in S3, automatically data will get loaded to staging via SnowPipe and these 2 rows of data will reside in stream as these are updated records. In another 1 minute, task will get called and 2 rows of data will be updated in Final table.

Row count of Staging and Final table will be different.

Delta Data Load Sample
----------------------
SNO,REPORTEDDATE,STATE,CONFIRMEDINDIANNATIONAL,CONFIRMEDFOREIGNNATIONAL,CURED,DEATHS,CONFIRMED
1,30-01-2020,Kerala,2,0,1,3,4
2,31-01-2020,Kerala,4,5,1,2,5

Note: Please observe 1st 2 rows data got changed.

# Data Transformation Using PySpark on Final Table

early covid-19 cases state wise for each columns. Schema would look like below.

Tables for storing Analytics Data in Snowflake
----------------------------------------------

Create database ETL_PROJECT_Analytics


Use ETL_PROJECT_Analytics;


Create or replace table monthly_covid_report(
    comonth varchar(15),
    coyear Number,
    state varchar(15),
    confirmedIndianNational Number,
    confirmedForeignNational Number,
    Cured Number,
    Deaths Number,
    Confirmed Number
)
comment = "This table consists of monthly data to track number of people getting covid belongs to india or not as well as number of deaths/cured/confirmed";


Create or replace table quarterly_covid_report(
    coquarter varchar(15),
    coyear Number,
    state varchar(15),
    confirmedIndianNational Number,
    confirmedForeignNational Number,
    Cured Number,
    Deaths Number,
    Confirmed Number
)
comment = "This table consists of quarterly data to track number of people getting covid belongs to india or not as well as number of deaths/cured/confirmed";



Create or replace table half_yearly_covid_report(
    cohalf varchar(15),
    coyear Number,
    state varchar(15),
    confirmedIndianNational Number,
    confirmedForeignNational Number,
    Cured Number,
    Deaths Number,
    Confirmed Number
)
comment = "This table consists of halfyearly data to track number of people getting covid belongs to india or not as well as number of deaths/cured/confirmed";

Verify the table creation using query 

Show Tables;

# AWS Glue Setup for running PySpark Jobs

To run PySpark Jobs, we need dependent Jars Spark Snowflake Connector and Snowflake Driver. Please download the dependent jars from here. We will be making use of this jar from the S3 bucket. So, please go ahead and create an S3 bucket and upload both the Jars as prerequisite.

1. In IAM > Roles > Create New Role > Choose Trusted Entity Type as AWS Service
   and select Glue as Use Cases for other service in the bottom of the page.

2. Attach policies with full access to S3 and Glue.

3. Proceed with default actions and create role. Hence, we are done with creation
   of role to call other AWS Services from Glue.

4. In AWS Glue > Jobs > Create > Job Details > Provide following details

  IAM Role - Newly Created IAM Role above
  Glue version - Glue 3.0
  Language - Python 3
  Worker type - G.1X
  Request number of workers - 2
  Number of retries - 3
  job timeout - 5 
  
5. Click on Save. We are ready to go for scripting in script tab.

# Data Transformation Script

INPUT > TRANSFORMATION (PYSPARK Aggregations) > OUTPUT
ETL_PROJECT_FINAL > DATA TRANSFORMATION > ETL_PROJECT_ANALYTICS

# Pyspark Code

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import to_date, month, year, date_format, sum, when, col


spark = SparkSession \
        .builder \
        .appName("Covid Data Transformation") \
        .getOrCreate()
        
def main():
    
    sn_src_name = "net.snowflake.spark.snowflake"
    
    ''' Databases Declaration '''
    sn_db_reading="ETL_PROJECT_FINAL"
    sn_db_writing="ETL_PROJECT_ANALYTICS"
    
    ''' Schema Declaration '''
    sn_schema="PUBLIC"
    
    ''' Database Tables for Reading and Writing Data From COVID_19 Tables'''
    src_tb_reading_c19="COVID_19"
    src_tb_writing_c19_monthly="MONTHLY_COVID_REPORT"
    src_tb_writing_c19_halfyearly="HALF_YEARLY_COVID_REPORT"
    src_tb_writing_c19_quarterly="QUARTERLY_COVID_REPORT"
    
    
    ''' Options For Reading Data '''
    sn_options_reading = {
        "sfUrl": "https://*********.snowflakecomputing.com",
        "sfUser": "***********",
        "sfPassword": "************",
        "sfDatabase": sn_db_reading,
        "sfSchema": sn_schema,
        "sfWarehouse": "COMPUTE_WH"
    }
    
    ''' Options For Writing Data '''
    sn_options_writing = {
        "sfUrl": "https://dm59907.us-east-2.aws.snowflakecomputing.com",
        "sfUser": "cheekushivam",
        "sfPassword": "Pediasure1#",
        "sfDatabase": sn_db_writing,
        "sfSchema": sn_schema,
        "sfWarehouse": "COMPUTE_WH"
    }
    
    df_covid_19 = spark.read \
        .format(sn_src_name) \
        .options(**sn_options_reading) \
    .option("dbtable",sn_db_reading+"."+sn_schema+"."+src_tb_reading_c19) \
        .load()
    
    # Transformation Dataframe Covid_19
    df_c19_monthly = df_covid_19.withColumn("REPORTEDDATE",to_date("REPORTEDDATE","dd-MM-yyyy"))
    df_c19_monthly = df_c19_monthly.withColumn('COMONTH',date_format('REPORTEDDATE','MMMM'))
    df_c19_monthly = df_c19_monthly.withColumn('COYEAR',year('REPORTEDDATE'))
    
    # Applying aggregate function to perform monthly sum on fields
    df_c19_monthly = df_c19_monthly.groupBy('COMONTH','COYEAR','STATE') \
    .agg(sum('CONFIRMEDINDIANNATIONAL').alias('CONFIRMEDINDIANNATIONAL'),sum('CONFIRMEDFOREIGNNATIONAL').alias('CONFIRMEDFOREIGNNATIONAL'), \
    sum('CURED').alias('CURED'),sum('DEATHS').alias('DEATHS'),sum('CONFIRMED').alias('CONFIRMED'))
    
    # Transferring Data to Target Table in Snowflake for covid_19 for monthly
    df_c19_monthly.write.format(sn_src_name) \
        .options(**sn_options_writing) \
        .option("dbtable", sn_db_writing+"."+sn_schema+"."+src_tb_writing_c19_monthly).mode("overwrite") \
        .save()
        
    print(" ***** Monthly Data Transfer Completed ******")
    
     # Applying aggregate function to perform quarterly sum on fields
    df_c19_quarterly = df_c19_monthly.withColumn("COQUARTER",when((col("COMONTH") == "January")|(col("COMONTH") == "February")|(col("COMONTH") == "March"),"First") \
    .when((col("COMONTH") == "April")|(col("COMONTH") == "May")|(col("COMONTH") == "June"),"Second") \
    .when((col("COMONTH") == "July")|(col("COMONTH") == "August")|(col("COMONTH") == "September"),"Third")
    .otherwise("Fourth"))
    
    df_c19_quarterly = df_c19_quarterly.groupBy('COQUARTER','COYEAR','STATE') \
    .agg(sum('CONFIRMEDINDIANNATIONAL').alias('CONFIRMEDINDIANNATIONAL'),sum('CONFIRMEDFOREIGNNATIONAL').alias('CONFIRMEDFOREIGNNATIONAL'), \
    sum('CURED').alias('CURED'),sum('DEATHS').alias('DEATHS'),sum('CONFIRMED').alias('CONFIRMED'))
    
    # Transferring Data to Target Table in Snowflake for covid_19 for monthly
    df_c19_quarterly.write.format(sn_src_name) \
        .options(**sn_options_writing) \
        .option("dbtable", sn_db_writing+"."+sn_schema+"."+src_tb_writing_c19_quarterly).mode("overwrite") \
        .save()
        
    print(" ***** Quarterly Data Transfer Completed ******")
    
    # Transformation Dataframe Covid_19 Half Yearly 
    df_c19_halfyearly = df_c19_monthly.withColumn("COHALF",when((col("COMONTH") == "January")|(col("COMONTH") == "February")|(col("COMONTH") == "March")| \
    (col("COMONTH") == "April")|(col("COMONTH") == "May")|(col("COMONTH") == "June"),"First") \
    .otherwise("Second"))
    
     # Applying aggregate function to perform half yearly sum on fields
    df_c19_halfyearly = df_c19_halfyearly.groupBy('COHALF','COYEAR','STATE') \
    .agg(sum('CONFIRMEDINDIANNATIONAL').alias('CONFIRMEDINDIANNATIONAL'),sum('CONFIRMEDFOREIGNNATIONAL').alias('CONFIRMEDFOREIGNNATIONAL'), \
    sum('CURED').alias('CURED'),sum('DEATHS').alias('DEATHS'),sum('CONFIRMED').alias('CONFIRMED'))
    
     # Transferring Data to Target Table in Snowflake for covid_19 for quarterly
    df_c19_halfyearly.write.format(sn_src_name) \
        .options(**sn_options_writing) \
        .option("dbtable", sn_db_writing+"."+sn_schema+"."+src_tb_writing_c19_halfyearly).mode("overwrite") \
        .save()
    
    print(" ***** Half Yearly Data Transfer Completed ******")
    
main()



# Learnings From This Article
1. How connection is setup between AWS SNS and Snowflake SQS?
2. Streams and Tasks?
3. How we can integrate PySpark and Snowflake using AWS Glue and dependent jars?
4. How to automate data transfer from S3 to Snowflake using Snowpipe with auto_ingest feature which works best with storage integration?
