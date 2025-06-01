from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder.appName("SalesReportsToClickHouse")
    .config(
        "spark.jars",
        "/jdbc-drivers/postgresql-42.7.5.jar,/jdbc-drivers/clickhouse-jdbc-0.4.6.jar",
    )
    .config(
        "spark.driver.extraJavaOptions",
        "-Dlog4j.configuration=file:///opt/bitnami/spark/conf/log4j2.properties",
    )
    .config(
        "spark.executor.extraJavaOptions",
        "-Dlog4j.configuration=file:///opt/bitnami/spark/conf/log4j2.properties",
    )
    .getOrCreate()
)


spark.sparkContext.setLogLevel("INFO")

pg_url = "jdbc:postgresql://postgres:5432/lab2"
pg_props = {"user": "spark", "password": "bomba", "driver": "org.postgresql.Driver"}

ch_url = "jdbc:clickhouse://clickhouse:8123/default"
ch_driver = "com.clickhouse.jdbc.ClickHouseDriver"
ch_props = {
    "driver": ch_driver,
    "socket_timeout": "300000",
    "connect_timeout": "5",
    "user": "default",
    "password": "",
}


def get_table_options(table_name, df):
    pk_columns = []
    if "product_sales" in table_name or "product_quality" in table_name:
        pk_columns = ["id"]
    elif "customer" in table_name:
        pk_columns = ["customer_id"]
    elif "store" in table_name:
        pk_columns = ["seller_id"]
    elif "time" in table_name:
        pk_columns = ["year", "month"]
    else:
        pk_columns = [df.columns[0]]

    order_by = ", ".join(pk_columns) if pk_columns else "tuple()"

    return f"ENGINE = MergeTree() ORDER BY ({order_by})"


countries = spark.read.jdbc(url=pg_url, table="countries", properties=pg_props)
cities = spark.read.jdbc(url=pg_url, table="cities", properties=pg_props)
stores = spark.read.jdbc(url=pg_url, table="stores", properties=pg_props)
pet_types = spark.read.jdbc(url=pg_url, table="pet_types", properties=pg_props)
pet_breeds = spark.read.jdbc(url=pg_url, table="pet_breeds", properties=pg_props)
pet_categories = spark.read.jdbc(
    url=pg_url, table="pet_categories", properties=pg_props
)

customers = spark.read.jdbc(
    url=pg_url, table="customers", properties=pg_props
).withColumnsRenamed(
    {
        "id": "customer_id",
        "first_name": "customer_first_name",
        "last_name": "customer_last_name",
        "country_id": "customer_country_id",
    }
)

sellers = spark.read.jdbc(
    url=pg_url, table="sellers", properties=pg_props
).withColumnsRenamed(
    {
        "id": "seller_id",
        "country_id": "seller_country_id",
    }
)

product_names = spark.read.jdbc(url=pg_url, table="product_names", properties=pg_props)
product_categories = spark.read.jdbc(
    url=pg_url, table="product_categories", properties=pg_props
)
product_sizes = spark.read.jdbc(url=pg_url, table="product_sizes", properties=pg_props)
product_colors = spark.read.jdbc(
    url=pg_url, table="product_colors", properties=pg_props
)
product_brands = spark.read.jdbc(
    url=pg_url, table="product_brands", properties=pg_props
)
product_materials = spark.read.jdbc(
    url=pg_url, table="product_materials", properties=pg_props
)


products = spark.read.jdbc(url=pg_url, table="products", properties=pg_props)


sales = spark.read.jdbc(
    url=pg_url, table="sales", properties=pg_props
).withColumnsRenamed(
    {
        "id": "sale_id",
    }
)


enriched_products = (
    products.join(product_names, "product_name_id")
    .join(product_categories, "product_category_id")
    .join(product_sizes, "product_size_id")
    .join(product_colors, "product_color_id")
    .join(product_brands, "product_brand_id")
    .join(product_materials, "product_material_id")
)


enriched_sales = (
    sales.join(enriched_products, sales.sale_product_id == enriched_products.id)
    .join(customers, sales.sale_customer_id == customers.customer_id)
    .join(sellers, sales.sale_seller_id == sellers.seller_id)
    .join(
        countries.alias("customer_countries"),
        customers.customer_country_id == F.col("customer_countries.country_id"),
    )
    .join(
        countries.alias("seller_countries"),
        sellers.seller_country_id == F.col("seller_countries.country_id"),
    )
    .withColumn("parsed_date", F.to_date("sale_date", "yyyy-MM-dd"))
    .withColumn("year", F.coalesce(F.year("parsed_date"), F.lit(1900)))
    .withColumn("month", F.coalesce(F.month("parsed_date"), F.lit(1)))
)


valid_sales = enriched_sales.filter((F.col("year") > 1900) & (F.col("year") < 2100))


product_sales = valid_sales.groupBy("id", "name", "category", "brand").agg(
    F.sum("sale_total_price").alias("total_revenue"),
    F.sum("sale_quantity").alias("total_quantity_sold"),
    F.avg("rating").alias("avg_rating"),
    F.sum("reviews").alias("total_reviews"),
)

top_products = product_sales.orderBy(F.desc("total_quantity_sold")).limit(10)

category_revenue = product_sales.groupBy("category").agg(
    F.sum("total_revenue").alias("category_revenue"),
    F.avg("avg_rating").alias("category_avg_rating"),
)


customer_sales = valid_sales.groupBy(
    "customer_id",
    "customer_first_name",
    "customer_last_name",
    "customer_countries.country",
).agg(
    F.sum("sale_total_price").alias("total_spent"),
    F.count("*").alias("purchase_count"),
    F.avg("sale_total_price").alias("avg_order_value"),
)

top_customers = customer_sales.orderBy(F.desc("total_spent")).limit(10)

customer_country_distribution = customer_sales.groupBy("country").agg(
    F.count("*").alias("customer_count"), F.sum("total_spent").alias("country_revenue")
)


time_analysis = valid_sales.groupBy("year", "month").agg(
    F.sum("sale_total_price").alias("monthly_revenue"),
    F.count("*").alias("order_count"),
    F.avg("sale_total_price").alias("avg_order_value"),
)


yoy_analysis = time_analysis.withColumn(
    "prev_year_revenue",
    F.lag("monthly_revenue").over(Window.partitionBy("month").orderBy("year")),
).withColumn(
    "yoy_growth",
    F.when(
        F.col("prev_year_revenue").isNotNull(),
        (
            (F.col("monthly_revenue") - F.col("prev_year_revenue"))
            / F.col("prev_year_revenue")
        )
        * 100,
    ).otherwise(0.0),
)


store_performance = valid_sales.groupBy("seller_id", "seller_countries.country").agg(
    F.sum("sale_total_price").alias("total_revenue"),
    F.count("*").alias("order_count"),
    F.avg("sale_total_price").alias("avg_order_value"),
)

top_stores = store_performance.orderBy(F.desc("total_revenue")).limit(5)

store_geo_distribution = store_performance.groupBy("country").agg(
    F.sum("total_revenue").alias("location_revenue"),
    F.sum("order_count").alias("location_orders"),
)


product_quality = (
    valid_sales.groupBy("id", "name", "category", "rating", "reviews")
    .agg(
        F.sum("sale_total_price").alias("total_revenue"),
        F.sum("sale_quantity").alias("total_quantity_sold"),
    )
    .withColumn(
        "revenue_per_rating",
        F.col("total_revenue")
        / F.when(F.col("rating") > 0, F.col("rating")).otherwise(1),
    )
)

top_rated_products = product_quality.orderBy(F.desc("rating")).limit(10)
lowest_rated_products = product_quality.orderBy("rating").limit(10)
most_reviewed_products = product_quality.orderBy(F.desc("reviews")).limit(10)


rating_sales_correlation = product_quality.select(
    F.coalesce(F.corr("rating", "total_quantity_sold"), F.lit(0.0)).alias(
        "rating_sales_correlation"
    )
).withColumn(
    "rating_sales_correlation",
    F.when(F.col("rating_sales_correlation").isNull(), 0.0).otherwise(
        F.col("rating_sales_correlation")
    ),
)


marts = [
    ("product_sales", product_sales),
    ("top_products", top_products),
    ("category_revenue", category_revenue),
    ("customer_sales", customer_sales),
    ("top_customers", top_customers),
    ("customer_country_distribution", customer_country_distribution),
    ("time_analysis", time_analysis),
    ("yoy_analysis", yoy_analysis),
    ("store_performance", store_performance),
    ("top_stores", top_stores),
    ("store_geo_distribution", store_geo_distribution),
    ("product_quality", product_quality),
    ("top_rated_products", top_rated_products),
    ("lowest_rated_products", lowest_rated_products),
    ("most_reviewed_products", most_reviewed_products),
    ("rating_sales_correlation", rating_sales_correlation),
]

for table_name, df in marts:
    try:
        record_count = df.count()

        if record_count == 0:
            continue

        table_options = get_table_options(table_name, df)

        df.write.format("jdbc").mode("overwrite").option("url", ch_url).option(
            "dbtable", table_name
        ).option("driver", ch_driver).option("socket_timeout", "300000").option(
            "connect_timeout", "5"
        ).option("createTableOptions", table_options).save()
    except Exception:
        pass


spark.stop()
