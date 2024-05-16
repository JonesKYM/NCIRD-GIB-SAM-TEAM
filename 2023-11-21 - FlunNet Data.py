# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

html= """<hl style="color:Blue;text-align:center;font-family:Arial">Global Influenza Analysis</hl>"""
displayHTML(html)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Flunet.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", True) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a new column called "B (Victoria)" by combining the specified columns
df = df.withColumn("B (Victoria)", 
                   df["BVIC_2DEL"] + df["BVIC_3DEL"] + df["BVIC_NODEL"] + df["BVIC_DELUNK"])

# Show the DataFrame with the new column
df.show()




# COMMAND ----------

from pyspark.sql.functions import col, expr

# Calculate the percentage using the formula
df = df.withColumn("Influenza A PP", (col("INF_ALL") / col("SPEC_PROCESSED_NB")) * 100)

# Calculate the percentage using the formula
df = df.withColumn("Influenza B PP", (col("INF_B") / col("SPEC_PROCESSED_NB")) * 100)

# Show the DataFrame with the new column
df.show()

# COMMAND ----------

from pyspark.sql.functions import col

# Create a new column called "B (Victoria)" by combining the specified columns
df = df.withColumn("B (Victoria)", 
                   df["BVIC_2DEL"] + df["BVIC_3DEL"] + df["BVIC_NODEL"] + df["BVIC_DELUNK"])

# Calculate the percentage using the formula
df = df.withColumn("Influenza A PP", (col("INF_ALL") / col("SPEC_PROCESSED_NB")) * 100)

# Calculate the percentage using the formula
df = df.withColumn("Influenza B PP", (col("INF_B") / col("SPEC_PROCESSED_NB")) * 100)

# Show the DataFrame with the new columns
df.show()

# COMMAND ----------

import matplotlib.pyplot as plt

# Assign the subtype counts to variables
AH1N12009_count = 979
AH1_count = 236
AH3_count = 916
AH5_count = 14
AH7N9_count = 8
ANOTSUBTYPABLE_count = 1315
INF_A_count = 1909
B_Victoria_count = 483
BYAM_count = 276
BNOTDETERMINED_count = 856
INF_B_count = 1016
INF_ALL_count = 2211

# Prepare the subtype names and counts as lists
subtype_names = ["AH1N12009", "AH1", "AH3", "AH5", "AH7N9", "ANOTSUBTYPABLE", "INF_A", "B (Victoria)", "BYAM", "BNOTDETERMINED", "INF_B", "INF_ALL"]
subtype_counts = [AH1N12009_count, AH1_count, AH3_count, AH5_count, AH7N9_count, ANOTSUBTYPABLE_count, INF_A_count, B_Victoria_count, BYAM_count, BNOTDETERMINED_count, INF_B_count, INF_ALL_count]

# Create a bar chart
plt.figure(figsize=(10, 6))
plt.bar(subtype_names, subtype_counts)
plt.xlabel("Subtypes")
plt.ylabel("Count")
plt.title("Epic Curve - Global Subtype Counts")
plt.xticks(rotation='vertical')
plt.show()

# COMMAND ----------

df.createOrReplaceTempView("my_table_view")
display(df)


# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.dropdown(name='WHO Regions',defaultValue='AMR', choices=['AFR', 'AMR', 'EMR', 'EUR', 'SEAR','WPR'], label = 'WHO Regions')

# COMMAND ----------

dbutils.widgets.dropdown(name='Hemisphere',defaultValue='NH', choices=['NH', 'SH',], label = 'Hemisphere')

# COMMAND ----------

dbutils.widgets.combobox(name='Area',defaultValue='Anguilla', choices=['Anguilla', 'Antigua and Barbuda', 'Aruba', 'Bahamas', 'Barbados','Belize', 'Bermuda', 'Bristish Virgin Islands', 'Canada', 'Cayman Islands'], label = 'Country, Area, or Territory')

# COMMAND ----------

# Create a view or table

temp_table_name = "Flunet_csv"



# COMMAND ----------

# Assuming you have a Spark DataFrame called 'df' with the first row as column names

# Get the first row as a list
first_row = df.first()

# Rename columns using toDF and pass the list as new column names
df = df.toDF(*first_row)

# Show the DataFrame with renamed columns
df.show()

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "Flunet_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)
