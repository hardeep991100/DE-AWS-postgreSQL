from resources.dev import config
from src.main.read.database_read import DatabaseReader
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.p_sql_session import *
from src.main.read.aws_read import *
from src.main.download.aws_file_download import *
from src.main.utility.spark_session import spark_session
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import concat_ws, lit
import shutil
from src.main.move.move_files import *
from src.main.transformations.jobs.dimension_tables_join import *
from src.main.write.parquet_writer import *
from src.main.upload.upload_to_s3 import *
from src.main.transformations.jobs.customer_mart_sql_tranform_write import *
from src.main.transformations.jobs.sale_mart_sql_transform_write import *
from src.main.delete.local_file_delete import *

import os
import datetime





# #####################Get S 3 client ##################
aws_access_key = config.aws_access_key
# aws_access_key = "BoDD3/AeE="
# aws_secret_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()


# Now you can use s3-client for your S3 operations
response = s3_client.list_buckets()
print(response)
logger.info("S3 Buckets: %s", response['Buckets'])


# check if local directory has already a file
# if file is there then check if the same file is present in the staging area
# with status as A. If so then don't delete and try to re-run
# Else give an error and not process the next file

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith('.csv')]
connection = get_psql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

        statement = f"""select distinct file_name from 
                    {config.database}.public.{config.product_staging_table}
                    where file_name in ({str(total_csv_files)[1:-1]}) and status = 'A'
                """
        logger.info(f"dynamically statement created: {statement}")
        cursor.execute(statement)
        data = cursor.fetchall()


        # if data:
        #     logger.error(f"File {file} is already in processing with status I. Cannot re-process the file.")
        #     continue
        # else:
        #     delete_query = f"""DELETE FROM dataengineer.public.product_staging_table
        #                     where file_name = '{file}' and status = 'A'
        #                 """
        #     logger.info(f"Delete query to remove old processed file entry: {delete_query}")
        #     cursor.execute(delete_query)
        #     connection.commit()
        #     logger.info(f"Old processed file entry for {file} deleted successfully.")

        if data:
            logger.info("Your last run was failed please check again.")
        else:
            logger.info("No record found.")

else:
    logger.info("Last run was successful!!!")

try: 
    s3reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3reader.list_files(s3_client, 
                                                 config.bucket_name,
                                                 folder_path = folder_path)
    
    logger.info("Absolute path on s3 bucket for csv file %s",s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files found in the specified {folder_path} directory.")
        raise Exception("No data available to process.")

except Exception as e:
    logger.error("Error occurred while listing files from S3: %s", str(e))
    raise e

bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logging.info("File path available on s3 under %s bucket and folder name is %s",bucket_name, file_paths)
logging.info(f"File path available on s3 under {bucket_name} bucket and folder name is {file_paths}")
try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("Error occurred while downloading files from S3: %s", str(e))
    # raise e
    sys.exit()


all_files = os.listdir(local_directory)
logger.info("All files downloaded from s3 to local directory %s",all_files)

if all_files:
    csv_files = []
    error_files = []
    for file in all_files:
        if file.endswith('.csv'):
            csv_files.append(os.path.abspath(os.path.join(local_directory, file)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, file)))
    
    if not csv_files:
        logger.error("No CSV files available to process the request.")
        raise Exception("No CSV files to process.")
else:
    logger.error("No files downloaded from s3 to local directory.")
    raise Exception("No files downloaded from S3.")



# ######################## make CSV files convert into list of coma separated ##################
# csv_files = str(csv_files)[1:-1]
logger.info("******************** Listing of files available in local directory ********************")
logger.info("CSV files available in local directory to be processed: %s",csv_files)
# logger.info("Error files available in local directory %s",error_files)

logger.info("******************** End of Listing of files available in local directory ********************")
logger.info("***************** Creating spark session ***********************")

spark = spark_session()

logger.info("***************** Spark session created successfully *****************")

logger.info("***************** Checking schema for data loaded in s3 *****************")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv") \
        .option("header", "true")\
        .load(data).columns
    logger.info("Schema for file %s is %s",data,data_schema)
    logger.info("Mandatory columns required %s",config.mandatory_columns)

    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info("Missing columns are %s",missing_columns)

    if missing_columns:
        error_files.append(data)
    else:
        logger.info("No missing columns for this %s",data)
        correct_files.append(data)

logger.info("No missing column, Correct files with all mandatory columns %s",correct_files)
logger.info("Error files with missing mandatory columns %s",error_files)
logger.info("***************** Moving error data to error directory if any *****************")


# Move error files to error directory
error_folder_path_local = config.error_folder_path_local

if error_files:
    for error_file in error_files:
        if os.path.exists(error_file):
            file_name = os.path.basename(error_file)
            destination_path = os.path.join(error_folder_path_local, file_name)

            shutil.move(error_file, destination_path)
            logger.info("Moved error file %s to error directory %s",error_file,destination_path)

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(
                s3_client,
                bucket_name,
                source_prefix,
                destination_prefix,
                file_name
            )
            logger.info(message)
        else:
            logger.error("Error file %s does not exist.", error_file)
else:
    logger.info("No error files to move to error directory.")

#         try:
#             error_file_name = os.path.basename(error_file)
#             error_destination_path = os.path.join(config.error_folder_path_local, error_file_name)
#             os.rename(error_file, error_destination_path)
#             logger.info("Moved error file %s to error directory %s",error_file,error_destination_path)
#         except Exception as e:
#             logger.error("Error occurred while moving file %s to error directory: %s",error_file, str(e))
#             # raise e
# else:
#     logger.info("No error files to move to error directory.")

logger.info("***************** End of Moving error data to error directory if any *****************")

# how to handle csv files with additional column

# But, before running the process
# stage table needs to be updated with status as Active(A) or inactive(I)

logger.info("***************** Update stage table that  we started the process *****************")

insert_statements = []

db_name = config.database_name
current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statement = f"""INSERT INTO {db_name}.public.{config.product_staging_table} 
                        (file_name, file_location, created_date, status) 
                        VALUES ('{filename}', '{file}', '{current_time}', 'A');
                    """
        insert_statements.append(statement)
    logger.info("Insert statement for staging table: %s",insert_statements)
    logger.info("********* Connecting to PostgresSQL database to update staging table *********")
    connection = get_psql_connection()
    cursor = connection.cursor()
    logger.info("********* Connected to PostgresSQL database successfully *********")

    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.info("There is no files to process in staging area.")
    raise Exception("No data available with the correct files.")

logger.info("***************** Staging table updated successfully that we started the process *****************")

logger.info("***************** fixing extra columns *****************")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("extra_column", StringType(), True)  # Example of an extra column
])

# final_df_to_process = spark.createDataFrame([], schema=schema)
# final_df_to_process.show()


# If above code failsconnecting to DatabaseReader
database_client = DatabaseReader(config.url, config.properties)
logger.info("DatabaseReader client created successfully.")

final_df_to_process = database_client.create_dataframe(spark, "empty_df_create_table")
# final_df_to_process.show()

for data in correct_files:

    data_df = spark.read.format("csv")\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info("Extra columns in the file are %s",extra_columns)

    if extra_columns:
        data_df = data_df.withColumn("extra_column", concat_ws(", ", *extra_columns))\
                    .select(config.mandatory_columns + ["extra_column"])
        logger.info("Processed %s and add extra columns into single column",data)
    else:
        data_df = data_df.withColumn("extra_column", lit(None))\
                    .select(config.mandatory_columns + ["extra_column"])
        logger.info("No extra columns found in %s",data)
        
    final_df_to_process = final_df_to_process.unionByName(data_df)

logger.info("Final dataframe to process after fixing extra columns:")
final_df_to_process.show()

#Enrich the data from all dimension table
#also create a datamart for sales _ team and their incentive, address and all
#another datamart for customer who bought how much each days of month
#for every month there should be a file and inside that
#there should be a store_id segregation
#Read the data from parquet and generate a csv file
#in which there will be a
#sales_person_total_bitling_done_for_each_month, total_incentive

#connecting to DB

database_client = DatabaseReader(config.url, config.properties)
logger.info("DatabaseReader client created successfully for dimension table read.")

# Read dimension tables
# customer table
logger.info("********* Loading Customer dimension table ********")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)

# product table
logger.info("********* Loading Product dimension table ********")
product_table_df = database_client.create_dataframe(spark, config.product_table)

# product staging table
logger.info("********* Loading Product Staging table ********")
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)

# sales team table
logger.info("********* Loading Sales Team dimension table ********")
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

# store table
logger.info("********* Loading Store dimension table ********")
store_table_df = database_client.create_dataframe(spark, config.store_table)

logger.info("***************** All dimension tables loaded successfully *****************")
# Perform necessary joins and transformations to create data marts

s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,
                                                        customer_table_df,
                                                        store_table_df,
                                                        sales_team_table_df)
logger.info("Fact table successfully created.")

print(s3_customer_store_sales_df_join.show())

#Write the customer data into customer data mort in parquet formot
# wilt be written to local first
#move the RAW data to s3 bucket for reporting tool
#Write reporting data into MySQL table also

# customer data mart
logger.info("***************** Writing customer data mart to local directory in parquet format *****************")
final_customer_data_mart_df = s3_customer_store_sales_df_join\
                                .select("ct.customer_id","ct.first_name","ct.last_name","ct.address","ct.pincode",
                                        "ct.phone_number","sales_date","total_cost")

logger.info("Final customer data mart dataframe:")
final_customer_data_mart_df.show()


# sales team data mart
logger.info("***************** Writing sales team data mart to local directory in parquet format *****************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
                                .select("store_id",
                                        "sales_person_id","sales_person_first_name","sales_person_last_name",
                                        "store_manager_name", "manager_id","is_manager",
                                        "sales_person_address","sales_person_pincode",
                                        "sales_date","total_cost",
                                        expr("substring(sales_date, 1, 7)").alias("sales_month"))

logger.info("Final sales team data mart dataframe:")
final_sales_team_data_mart_df.show()


parquet_writer = ParquetWriter(mode="overwrite", data_format="parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)

# logger.info(f"Customer data mart written successfully to local directory ({config.customer_data_mart_local_file}) in parquet format.")

# uploading to s3 customer data mart
logger.info("***************** Uploading customer data mart parquet files to S3 *****************")
s3_directory = config.s3_customer_datamart_directory
s3_uploader = UploadToS3(s3_client)



message = s3_uploader.upload_to_s3(s3_directory,
                                   config.bucket_name,
                                   config.customer_data_mart_local_file)

# logger.info(message)
logger.info("***************** Customer data mart parquet files uploaded to S3 successfully *****************")


parquet_writer.dataframe_writer(final_sales_team_data_mart_df, config.sales_team_data_mart_local_file)
logger.info(f"Sales team data mart written successfully to local directory ({config.sales_team_data_mart_local_file}) in parquet format.")


# uploading to s3 sales team data mart
logger.info("***************** Uploading sales team data mart parquet files to S3 *****************")
s3_directory = config.s3_sales_datamart_directory
s3_uploader = UploadToS3(s3_client)
message = s3_uploader.upload_to_s3(s3_directory,
                                   config.bucket_name,
                                   config.sales_team_data_mart_local_file)


# logger.info(message)
logger.info("***************** Sales team data mart parquet files uploaded to S3 successfully *****************")



# also writing the data into partitioned parquet file based on store_id and sales_date months
final_sales_team_data_mart_df.write.format("parquet")\
    .option("header", "true")\
    .mode("overwrite")\
    .partitionBy("sales_month", "store_id")\
    .option("path", config.sales_team_data_mart_partitioned_local_file)\
    .save()
# can make the parquet writer class also for thisto be dynamic where it can take partition columns as input.

s3_prefix = "sales_partitoned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp())*1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)

        local_file_path = os.path.join(root, file)
        relative_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_path}"
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

logger.info("***************** Sales team partitioned data mart parquet files uploaded to S3 successfully *****************")

#calcutation for customer mart
#find out the customer total purchase every month
#write the data into MySQL table
logger.info("********** Customer every month purchased amount ***********")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("********** Customer every month purchased amount written to Postgres table successfully ***********")


#calculation for sales team mart
#find out the total sales done by each sales person
#Give the top performer 1% incentive of total sales
#Rest sales person will get nothing
#write the data into MySQL table
logger.info("********** Sales team every month total sales ***********")
sales_person_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info("********** Sales team every month total sales written to MySQL table successfully ***********")

# ################ Now delete the local files after processing is done ##################

source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory

message = move_s3_to_s3(s3_client, bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")

logger.info("*********** Deleting local files after processing is done ***********")
delete_local_file(config.local_directory)

delete_local_file(config.customer_data_mart_local_file)

delete_local_file(config.sales_team_data_mart_local_file)

delete_local_file(config.sales_team_data_mart_partitioned_local_file)

logger.info("*********** Local files deleted successfully ***********")

# changing the status in staging table from A to I after processing is done


update_statements = []
current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statement = f'''
                    UPDATE {db_name}.public.{config.product_staging_table}
                    SET status = 'I', updated_date = '{current_time}'
                    WHERE file_name = '{filename}';
                    '''
        update_statements.append(statement)
    logger.info("Update statement for staging table: %s",update_statements)
    logger.info("********* Connecting to PostgresSQL database to update staging table *********")
    connection = get_psql_connection()
    cursor = connection.cursor()
    logger.info("********* Connected to PostgresSQL database successfully *********")
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()

else:
    logger.info("There is no files to process in staging area to update status.")
    sys.exit()
    
logger.info("***************** Staging table updated successfully that we completed the process *****************")

# input("Press Enter to exit...")

