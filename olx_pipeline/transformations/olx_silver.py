from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window

file_path = f"/Volumes/workspace/default/olx_flats_data"

@dp.materialized_view(
    comment='Cleaned data from ads',
)
@dp.expect_or_drop("id", "id IS NOT NULL")
@dp.expect_or_drop("valid_day", "day BETWEEN 1 AND 31")
@dp.expect_or_drop("valid_month", "month BETWEEN 1 AND 12")
@dp.expect_or_drop("valid_price", "price_uah > 3000")
@dp.expect_or_drop("valid_size", "size_m2 >= 15")
def olx_silver():
    months_map = create_map([lit(x) for pair in [
        ("січня", "01"), ("лютого", "02"), ("березня", "03"), ("квітня", "04"),
        ("травня", "05"), ("червня", "06"), ("липня", "07"), ("серпня", "08"),
        ("вересня", "09"), ("жовтня", "10"), ("листопада", "11"), ("грудня", "12")
    ] for x in pair])
    is_today = col('location').contains('Сьогодні')
    scraped_date = to_date(col('time_collected'))
    iqr_window = Window.partitionBy('district', 'size_category')

    return (spark.table("olx_raw")
        .dropDuplicates(['id'])
        .withColumn("city", trim(regexp_extract("location", r"^([^,]+)", 1)))
        .withColumn("district", trim(regexp_extract("location", r",\s*([^-]+)", 1)))
        .withColumn("time", 
            when(regexp_extract("location", r"(\d{2}:\d{2})", 1) != "", 
                 regexp_extract("location", r"(\d{2}:\d{2})", 1))
        )
        .withColumn("clean_date", 
            when(is_today, scraped_date)
            .otherwise(to_date(concat_ws("-",
                regexp_extract("location", r"(\d{4})", 1),
                months_map[regexp_extract("location", r"\d+\s+([а-яіїєґ]+)", 1)],
                lpad(regexp_extract("location", r"-\s*(\d+)", 1), 2, "0")
            ), "yyyy-MM-dd"))
        )
        .withColumn("year", year("clean_date"))
        .withColumn("month", month("clean_date"))
        .withColumn("day", dayofmonth("clean_date"))
        .withColumn("price_uah", 
            regexp_replace(
                regexp_extract("price", r"^([\d\s]+)", 1), 
                r"\s+", ""
            ).cast("int")
        )
        .withColumn("size_m2", regexp_extract("size", r"(\d+)", 1).cast("float"))
        .withColumn('size_category', 
            when(col('size_m2') <= 35, 'Smart')
            .when((col('size_m2') > 35) & (col('size_m2') <= 50), 'Standart 1 room')
            .when((col('size_m2') > 50) & (col('size_m2') <= 70), 'Standart 2 rooms')
            .when((col('size_m2') > 70) & (col('size_m2') <= 100), 'Standart 3 rooms')
            .when((col('size_m2') > 100) & (col('size_m2') <= 130), 'Standart 4 rooms')
            .otherwise('Luxury'))
        .withColumn("Q1", expr("percentile_approx(price_uah, 0.25)").over(iqr_window))
        .withColumn("Q3", expr("percentile_approx(price_uah, 0.75)").over(iqr_window))
        .withColumn("IQR", col("Q3") - col("Q1"))
        .withColumn("upper_bound", col("Q3") + 3 * col("IQR"))
        .withColumn("lower_bound", col("Q1") - 1.5 * col("IQR"))
        .filter(
            ((col('price_uah') <= col('upper_bound')) | 
            (col('price_uah') <= 100000))
            &
            ((col('price_uah') > col('lower_bound')))
        )
        .select(
            "id", "title", "price_uah", "size_m2", "size_category",
            "city", "district", "time", "day", "month", "year", "clean_date",
            "url", "source", 
            col("time_collected").cast("timestamp").alias("scraped_at")
        )
    )