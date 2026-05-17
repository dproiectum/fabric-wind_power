# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Preparation

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to the table in the Silver Lakehouse
silver_table_path = "abfss://portfolio_WindPower@onelake.dfs.fabric.microsoft.com/LH_WindPower_Silver.Lakehouse/Tables/dbo/wind_power"

# Load the table into a DataFrame
df = spark.read.format("delta").load(silver_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create the Dimension Tables

# MARKDOWN ********************

# ## Dim table : Date

# CELL ********************

date_dim = date_dim.limit(0)
date_dim

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_dim = (df
    .select(
        "date",
        "day",
        "month",
        "quarter",
        "year",
    )
    .distinct()                                 #distinct() ensures we get one row per day (instead of duplicates)
    .withColumnRenamed("date", "date_id")
)   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(date_dim.head(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dim table : time

# MARKDOWN ********************


# CELL ********************

time_dim = (df
    .select(
        "time",
        "hour_of_day",
        "minute_of_hour",
        "second_of_minute",
        "time_period",
    )
    .distinct()
    .withColumnRenamed("time", "time_id")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(time_dim)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dim table : Turbine

# CELL ********************

turbine_dim = (df
    .select(
        "turbine_name"
        ,"capacity"
        ,"location_name"
        ,"latitude"
        , "longitude"
        ,"region"
    )
    .distinct()
    .withColumn(
        "turbine_id",
        row_number().over(Window.orderBy("turbine_name", "capacity","location_name", "latitude", "longitude", "region"))
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(turbine_dim)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dim table : Operational Status

# CELL ********************

operational_status_dim = (df
    .select(
        "status"
        ,"responsible_department"
    )
    .distinct()
    .withColumn(
        "status_id"
        ,row_number().over(Window.orderBy("status", "responsible_department"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fact table : WindPower_Production

# CELL ********************

# Join the dimension tables to the original DataFrame

df = (df
    .join(
        turbine_dim
        ,["turbine_name", "capacity", "location_name", "latitude", "longitude","region"]
        ,"left"
    )
    .join(
        operational_status_dim
        ,["status", "responsible_department"]
        ,"left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_table = (df
    .select(
        "production_id"
        ,"date"
        ,"time"
        ,"turbine_id"
        ,"status_id"
        ,"wind_direction"
        ,"wind_speed"
        ,"energy_produced"
    )
    .withColumnRenamed("date", "date_id")
    .withColumnRenamed("time", "time_id")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fact_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Save the tables in the Golden LakeHouse

# MARKDOWN ********************

# ## Path preparation

# CELL ********************

GOLD_ABFS_PATH = 'abfss://portfolio_WindPower@onelake.dfs.fabric.microsoft.com/LH_WindPower_Gold.Lakehouse/Tables'


gold_date_dim_path = GOLD_ABFS_PATH + "/dbo/dim_date"

gold_time_dim_path = GOLD_ABFS_PATH + "/dbo/dim_time"

gold_turbine_dim_path = GOLD_ABFS_PATH + "/dbo/dim_turbine"

gold_operational_status_dim_path = GOLD_ABFS_PATH + "/dbo/dim_operational_status"

gold_fact_windpower_path = GOLD_ABFS_PATH + "/dbo/Fact_WindPower"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#test
print(gold_time_dim_path)
print(gold_fact_windpower_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load to Golden Lakehouse

# CELL ********************

# Save the tables in the Gold Lakehouse

date_dim.write.format("delta").mode("overwrite").save(gold_date_dim_path)

time_dim.write.format("delta").mode("overwrite").save(gold_time_dim_path)

turbine_dim.write.format("delta").mode("overwrite").save(gold_turbine_dim_path)

operational_status_dim.write.format("delta").mode("overwrite").save(gold_operational_status_dim_path)

fact_table.write.format("delta").mode("overwrite").save(gold_fact_windpower_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fact_table.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
