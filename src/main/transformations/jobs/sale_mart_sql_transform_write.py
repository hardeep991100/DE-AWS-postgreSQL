from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_write import DatabaseWriter


def sales_person_mart_calculation_table_write(final_sales_team_data_mart_df):
    window = Window.partitionBy("store_id","sales_person_id","sales_month")
    final_sales_team_data_mart = final_sales_team_data_mart_df\
                                    .withColumn("sales_month", substring(col("sales_date"),1,7))\
                                    .withColumn("total_sales_every_month", 
                                                sum(col("total_cost")).over(window))\
                                    .select("store_id","sales_person_id",
                                            concat(col("sales_person_first_name"),
                                                   lit(" "),
                                                   col("sales_person_last_name")).alias("full_name"),
                                            "sales_month",
                                            col("total_sales_every_month").alias("total_sales"))\
                                    .distinct()
    # final_sales_team_data_mart.show()

    ranked_window = Window.partitionBy("store_id","sales_month").orderBy(col("total_sales").desc())
    final_sales_team_data_mart_table =  final_sales_team_data_mart\
                                            .withColumn("rnk", rank().over(ranked_window))\
                                            .withColumn("incentive", when(col("rnk") == 1, 
                                                                          col("total_sales") * 0.10)\
                                                                          .otherwise(lit(0)))\
                                                                          .withColumn("incentive", round(col("incentive"),2))\
                                                                .select("store_id",
                                                                        "sales_person_id",
                                                                        "full_name",
                                                                        "sales_month",
                                                                        "total_sales",
                                                                        "incentive")
    

    print("Writing the Final Sales Person Data Mart Table")
    #Write the Data into PostgresSQL sales_team_data_mart table
    db_writer = DatabaseWriter(url=config.url,properties=config.properties)
    db_writer.write_dataframe(final_sales_team_data_mart_table,
                              config.sales_team_data_mart_table)



