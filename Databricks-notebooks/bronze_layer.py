from pyspark.sql.functions import *

# Azure Event Hub Configuration 
# connection of our Databricksnotebook to eventhub
event_hub_namespace = "hospital-analytics-name-space.servicebus.windows.net"
event_hub_name="hospital-analytics-eventhub"  
event_hub_conn_str = dbutils.secrets.get(scope = "hospitalanalysisvaultscope", key = "Eventhub-secret-connection-string")

#Hospitalanalysisvaultscope
kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}
#Read from eventhub
# Reding the data from Event Hub
raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
          )

#Cast data to json
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

# now we have the data in json format we can write it to adls lake
#ADLS configuration 
# so we need to get the permission to adls lake
spark.conf.set(
  "fs.azure.account.key.hospitalstorageacc.dfs.core.windows.net",
  dbutils.secrets.get(scope = "hospitalanalysisvaultscope", key = "storage-connection-secret")
)

# Define bronze path to store raw data
bronze_path = "abfss://bronze@hospitalstorageacc.dfs.core.windows.net/patient_flow"

#Write stream to bronze
(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "abfss://bronze@hospitalstorageacc.dfs.core.windows.net/patient_flow/_checkpoints"
    )
    .start(bronze_path)
)