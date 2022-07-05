# Pyspark Code
# ------------

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
        "sfUrl": "https://**********.aws.snowflakecomputing.com",
        "sfUser": "************",
        "sfPassword": "**********",
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
