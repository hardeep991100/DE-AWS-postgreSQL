# import findspark
# findspark.init()
# from pyspark.sql import SparkSession
# from pyspark.sql import *
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from src.main.utility.logging_config import *
#
# def spark_session():
#     spark = SparkSession.builder.master("local[*]") \
#         .appName("manish_spark2")\
#         .config("spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-java-8.0.26.jar") \
#         .getOrCreate()
#     logger.info("spark session %s",spark)
#     return spark

# #OG:
# import findspark
# findspark.init()
# from pyspark.sql import SparkSession
# from pyspark.sql import *
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from src.main.utility.logging_config import *
#
# # .config("spark.jars",
#                 # r"C:\Users\naivi\OneDrive\Desktop\Practice\data_engineer\DataEngineeringProject\postgresql-42.7.8.jar")
# def spark_session():
#     spark = SparkSession.builder.master("local[*]") \
#         .appName("hardeep_spark2")\
#         .config("spark.driver.extraClassPath",
#                 r"C:\Users\naivi\OneDrive\Desktop\Practice\data_engineer\DataEngineeringProject\postgresql-42.7.8.jar") \
#         .getOrCreate()
#     logger.info("spark session %s",spark)
#     return spark


# import findspark
# findspark.init()

# from pyspark.sql import SparkSession
# from src.main.utility.logging_config import logger

# def spark_session():
#     spark = (
#         SparkSession.builder
#         .master("local[*]")
#         .appName("hardeep_spark2")

#         # PostgreSQL JDBC driver
#         .config(
#             "spark.driver.extraClassPath",
#             r"C:\Users\naivi\OneDrive\Desktop\Practice\data_engineer\DataEngineeringProject\postgresql-42.7.8.jar"
#         )

#         # 🔴 CRITICAL: Disable native Hadoop on Windows
#         .config("spark.hadoop.io.native.lib.available", "false")
#         .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")

#         # 🔴 Avoid warehouse permission issues
#         .config("spark.sql.warehouse.dir", "file:/C:/tmp/spark-warehouse")

#         # Optional but good practice
#         .config("spark.sql.adaptive.enabled", "true")
#         .config("spark.sql.shuffle.partitions", "8")

#         .getOrCreate()
#     )

#     logger.info("Spark session created successfully")
#     return spark


# from pyspark.sql import SparkSession
# from src.main.utility.logging_config import logger

# def spark_session():
#     spark = (
#         SparkSession.builder
#         .appName("de_project")
#         .getOrCreate()
#     )
#     logger.info("Spark session created successfully")
#     return spark


from pyspark.sql import SparkSession
from src.main.utility.logging_config import logger

def spark_session():
    spark = (
        SparkSession.builder
        .appName("de_project")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .getOrCreate()
    )

    logger.info("Spark session created successfully")
    return spark
