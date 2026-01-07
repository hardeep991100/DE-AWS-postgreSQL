import os

key = "youtube_project"
iv = "youtube_encyptyo"
salt = "youtube_AesEncryption"

#AWS Access And Secret key
aws_access_key = "xt14r14o+XltHcPuZGKw/YWfTPp9q8YV/HT7n4MMi9s="
aws_secret_key = "4W1W4V9rsBZA+TT6ScFWySBooFAIgeS234tv/Jcc7cz6PQnak+GpGpr6h/8Pojc1"
bucket_name = "de-project-hardeep"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
# database_name = "youtube_project"
# url = f"jdbc:mysql://localhost:3306/{database_name}"
# properties = {
#     "user": "root",
#     "password": "password",
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

#Database credential for PostgresSQL
# localhost = "localhost"
# port = 5432
# user = "postgres"
# password = "201095"
# database = "dataengineer"


localhost = os.getenv("DB_HOST", "host.docker.internal")
port = int(os.getenv("DB_PORT", 5432))
user = os.getenv("DB_USER", "postgres")
password = os.getenv("DB_PASSWORD", "201095")
database = os.getenv("DB_NAME", "dataengineer")


# PostgresSQL database connection properties
database_name = "dataengineer"

url = f"jdbc:postgresql://{localhost}:{port}/{database_name}"

properties = {
    "user": user,          # or your postgres user
    "password": password,
    "driver": "org.postgresql.Driver"
}

# Table name in on-premise database and data mart details
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id",
                     "store_id",
                     "product_name",
                     "sales_date",
                     "sales_person_id",
                     "price",
                     "quantity",
                     "total_cost"]

# C:\Users\naivi\OneDrive\Desktop\Practice\data_engineer\DataEngineeringProject\PycharmDEProject
# File Download location
# local_directory = "C:\\Users\\naivi\\OneDrive\\Desktop\\Practice\\data_engineer\\DataEngineeringProject\\PycharmDEProject\\data\\file_from_s3\\"
# customer_data_mart_local_file = "C:\\Users\\naivi\\OneDrive\\Desktop\\Practice\\data_engineer\\DataEngineeringProject\\PycharmDEProject\\data\\customer_data_mart\\"
# sales_team_data_mart_local_file = "C:\\Users\\naivi\\OneDrive\\Desktop\\Practice\\data_engineer\\DataEngineeringProject\\PycharmDEProject\\data\\sales_team_data_mart\\"
# sales_team_data_mart_partitioned_local_file = "C:\\Users\\naivi\\OneDrive\\Desktop\\Practice\\data_engineer\\DataEngineeringProject\\PycharmDEProject\\data\\sales_partition_data\\"
# error_folder_path_local = "C:\\Users\\naivi\\OneDrive\\Desktop\\Practice\\data_engineer\\DataEngineeringProject\\PycharmDEProject\\data\\error_files\\"


LOCAL_BASE_PATH = os.getenv("LOCAL_BASE_PATH", "/opt/spark-data")

local_directory = f"{LOCAL_BASE_PATH}/file_from_s3"
error_folder_path_local = f"{LOCAL_BASE_PATH}/error_files"
customer_data_mart_local_file = f"{LOCAL_BASE_PATH}/customer_data_mart"
sales_team_data_mart_local_file = f"{LOCAL_BASE_PATH}/sales_team_data_mart"
sales_team_data_mart_partitioned_local_file = f"{LOCAL_BASE_PATH}/sales_partition_data"

