from pyspark import pipelines as dp
from pyspark.sql.functions import *

file_path = f"/Volumes/workspace/default/olx_flats_data"

@dp.materialized_view(comment = "Dimension for unique locations")
def dim_location():
  return(spark.table('olx_silver')
         .select('city', 'district')
         .distinct()
         .withColumn('location_id', monotonically_increasing_id())
         .select('location_id', 'city', 'district')
  )
  
@dp.materialized_view(comment='Dimension for unique dates')
def dim_date():
  return(spark.table('olx_silver')
         .select('clean_date')
         .distinct()
         .withColumn('date_id', date_format(col('clean_date'), 'yyyyMMdd').cast('int'))
         .withColumn('year', year('clean_date'))
         .withColumn('month', month('clean_date'))
         .withColumn('day', dayofmonth('clean_date'))
         .withColumn('quarter', quarter('clean_date'))
         .withColumn('day_of_week', dayofweek('clean_date'))
         .select('date_id', 'clean_date', 'year', 'month', 'day', 'quarter', 'day_of_week')
  )

@dp.materialized_view(comment="Dimension for info")
def dim_info():
  return(spark.table('olx_silver')
         .select('id', 'title', 'url', 'source')
         .withColumnRenamed('id', 'olx_id')
         .distinct()
         .withColumn('info_id', monotonically_increasing_id())
         .select('info_id', 'olx_id', 'title', 'url', 'source')
  )

@dp.materialized_view(comment="Fact table for ads")
def fact_ads():
  base = spark.table('olx_silver')
  dim_loc = spark.table('dim_location')
  dim_date = spark.table('dim_date')
  dim_info = spark.table('dim_info')
  return(base
         .join(dim_loc, ['city', 'district'], 'left')
         .join(dim_date, ['clean_date'], 'left')
         .join(dim_info, base.id == dim_info.olx_id, 'left')
         .select('location_id', 'date_id', 'info_id', 'price_uah', 'size_m2')
         .withColumn('size_category', 
              when(col('size_m2') <= 35, 'Smart')
              .when((col('size_m2') > 35) & (col('size_m2') <= 50), 'Standart 1 room')
              .when((col('size_m2') > 50) & (col('size_m2') <= 70), 'Standart 2 rooms')
              .when((col('size_m2') > 70) & (col('size_m2') <= 100), 'Standart 3 rooms')
              .when((col('size_m2') > 100) & (col('size_m2') <= 130), 'Standart 4 rooms')
              .otherwise('Luxury'))
         .withColumn('ads_id', monotonically_increasing_id())
         .withColumn('price_per_m2', round(col('price_uah')/col('size_m2'),2))
         .select('ads_id', 'location_id', 'date_id', 'info_id', 'price_uah', 'size_m2', 'price_per_m2', 'size_category')
  )