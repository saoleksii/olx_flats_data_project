from pyspark import pipelines as dp
from pyspark.sql.functions import *

file_path = f"/Volumes/workspace/default/olx_flats_data"
schema_location = f"/Volumes/workspace/default/olx_flats_data/schema"

@dp.table(
    comment="raw ad data from scraper"
)
def olx_raw():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("multiline", "true")
            .load(file_path))