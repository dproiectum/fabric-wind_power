# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c842302b-f1d1-44da-85dc-3686f6839360",
# META       "default_lakehouse_name": "LH_WindPower_Bronze",
# META       "default_lakehouse_workspace_id": "8be01936-0e30-4acf-84c6-acc449b91be7",
# META       "known_lakehouses": [
# META         {
# META           "id": "c842302b-f1d1-44da-85dc-3686f6839360"
# META         },
# META         {
# META           "id": "80f123c8-50c5-4da8-82f8-b15feb52c01b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Load to df

# CELL ********************

# Path to the wind_power table in the Bronze Lakehouse
bronze_table_path = "abfss://portfolio_WindPower@onelake.dfs.fabric.microsoft.com/LH_WindPower_Bronze.Lakehouse/Tables/dbo/windpower"


# Load the wind_power table into a DataFrame
df = spark.read.format("delta").load(bronze_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display statements are useful during development. Once your notebook is production-ready : because we need to know the result
# the display statemenst should be remove in the production

#display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform to bronze data

# CELL ********************

from pyspark.sql.functions import (
col, round,
dayofmonth, month, quarter, year,
regexp_replace, substring, when
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean and enrich data
df_transformed = (
    df
    .withColumn("wind_speed", round(col("wind_speed"), 2))
    .withColumn("energy_produced", round(col("energy_produced"), 2))
    .withColumn("day", dayofmonth(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("quarter", quarter(col("date")))
    .withColumn("year", year(col("date")))
    .withColumn("time", regexp_replace(col("time"), "-", ":"))
    .withColumn("hour_of_day", substring(col("time"), 1, 2).cast("int"))
    .withColumn("minute_of_hour", substring(col("time"), 4, 2).cast("int"))
    .withColumn("second_of_minute", substring(col("time"), 7, 2).cast("int"))
    .withColumn("time_period",
        when((col("hour_of_day") >= 5) & (col("hour_of_day") < 12),"Morning")
        .when((col("hour_of_day") >= 12) & (col("hour_of_day") < 17),"Afternoon")
        .when((col("hour_of_day") >= 17) & (col("hour_of_day") < 21),"Evening")
        .otherwise("Night")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display statements are useful during development. Once your notebook is production-ready : because we need to know the result
# the display statemenst should be remove in the production


#display(df_transformed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Save the transformed df to Silver Lakehouse

# CELL ********************

silver_table_path = "abfss://portfolio_WindPower@onelake.dfs.fabric.microsoft.com/LH_WindPower_Silver.Lakehouse/Tables/dbo/wind_power"

# Save the transformed table to the Silver Lakehouse
df_transformed.write.format("delta").mode("overwrite").save(silver_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
