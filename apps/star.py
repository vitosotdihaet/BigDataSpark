import pandas as pd
from pyspark.sql import SparkSession

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 30)

DATA_DIR = "/data"

DATA_FILENAME = "MOCK_DATA"
df = pd.read_csv(f"{DATA_DIR}/{DATA_FILENAME}.csv")

for i in range(1, 10):
    t = pd.read_csv(f"{DATA_DIR}/{DATA_FILENAME} ({i}).csv")
    t[[c for c in t.columns if "id" in c.lower()]] += 1000 * i
    t.index = t.index + 1000 * i
    df = pd.concat([df, t])

country_cols = [c for c in df.columns if "country" in c.lower()]
countries = pd.DataFrame(
    {"country": pd.concat([df[c] for c in country_cols]).dropna().unique()}
)
countries["country_id"] = countries.index + 1

city_cols = [c for c in df.columns if "city" in c.lower()]
cities = pd.DataFrame({"city": pd.concat([df[c] for c in city_cols]).dropna().unique()})
cities["city_id"] = cities.index + 1

stores = df[[c for c in df.columns if "store" in c.lower()]].rename(
    columns={
        "store_name": "name",
        "store_location": "location",
        "store_city": "city",
        "store_state": "state",
        "store_country": "country",
        "store_phone": "phone",
        "store_email": "email",
    }
)
stores = stores.drop_duplicates()
stores = stores.merge(countries, on="country", how="left").drop(columns="country")
stores = stores.merge(cities, on="city", how="left").drop(columns="city")
stores["store_id"] = stores.index + 1

pet_types = (
    df[["customer_pet_type"]]
    .drop_duplicates()
    .rename(columns={"customer_pet_type": "type"})
    .reset_index()
    .drop(columns="index")
)
pet_types["pet_type_id"] = pet_types.index + 1

pet_breeds = (
    df[["customer_pet_breed"]]
    .drop_duplicates()
    .rename(columns={"customer_pet_breed": "breed"})
    .reset_index()
    .drop(columns="index")
)
pet_breeds["pet_breed_id"] = pet_breeds.index + 1

pet_categories = (
    df[["pet_category"]]
    .drop_duplicates()
    .rename(columns={"pet_category": "category"})
    .reset_index()
    .drop(columns="index")
)
pet_categories["pet_category_id"] = pet_categories.index + 1

customers = df[
    [c for c in df.columns if "customer" in c.lower() or "pet" in c.lower()]
].rename(
    columns={
        "sale_customer_id": "id",
        "customer_first_name": "first_name",
        "customer_last_name": "last_name",
        "customer_age": "age",
        "customer_email": "email",
        "customer_postal_code": "postal_code",
        "customer_pet_name": "pet_name",
    }
)
customers = customers.merge(
    pet_types, left_on="customer_pet_type", right_on="type", how="left"
).drop(columns=["customer_pet_type", "type"])
customers = customers.merge(
    pet_breeds, left_on="customer_pet_breed", right_on="breed", how="left"
).drop(columns=["customer_pet_breed", "breed"])
customers = customers.merge(
    pet_categories, left_on="pet_category", right_on="category", how="left"
).drop(columns=["pet_category", "category"])
customers = customers.merge(
    countries, left_on="customer_country", right_on="country", how="left"
).drop(columns=["customer_country", "country"])

sellers = df[[c for c in df.columns if "seller" in c.lower()]].rename(
    columns={
        "sale_seller_id": "id",
        "seller_first_name": "first_name",
        "seller_last_name": "last_name",
        "seller_email": "email",
        "seller_postal_code": "postal_code",
    }
)
sellers = sellers.merge(
    countries, left_on="seller_country", right_on="country", how="left"
).drop(columns=["seller_country", "country"])

suppliers = df[[c for c in df.columns if "supplier" in c.lower()]].rename(
    columns={
        "supplier_name": "name",
        "supplier_contact": "contact",
        "supplier_email": "email",
        "supplier_phone": "phone",
        "supplier_address": "address",
        "supplier_country": "country",
        "supplier_city": "city",
    }
)
suppliers = suppliers.merge(countries, on="country", how="left").drop(columns="country")
suppliers = suppliers.merge(cities, on="city", how="left").drop(columns="city")
suppliers["id"] = suppliers.index + 1


products = df[[c for c in df.columns if "product" in c.lower()]].rename(
    columns={
        "sale_product_id": "product_id",
        "product_name": "name",
        "product_category": "category",
        "product_price": "price",
        "product_quantity": "quantity",
        "product_weight": "weight",
        "product_color": "color",
        "product_size": "size",
        "product_brand": "brand",
        "product_material": "material",
        "product_description": "description",
        "product_rating": "rating",
        "product_reviews": "reviews",
        "product_release_date": "release_date",
        "product_expiry_date": "expiry_date",
    }
)

product_names = products[["name"]].drop_duplicates().reset_index()[["name"]]
product_names["product_name_id"] = product_names.index + 1

product_categories = (
    products[["category"]].drop_duplicates().reset_index()[["category"]]
)
product_categories["product_category_id"] = product_categories.index + 1

product_sizes = products[["size"]].drop_duplicates().reset_index()[["size"]]
product_sizes["product_size_id"] = product_sizes.index + 1

product_colors = products[["color"]].drop_duplicates().reset_index()[["color"]]
product_colors["product_color_id"] = product_colors.index + 1

product_brands = products[["brand"]].drop_duplicates().reset_index()[["brand"]]
product_brands["product_brand_id"] = product_brands.index + 1

product_materials = products[["material"]].drop_duplicates().reset_index()[["material"]]
product_materials["product_material_id"] = product_materials.index + 1

products = products.merge(product_names, on="name", how="left").drop(columns=["name"])
products = products.merge(product_categories, on="category", how="left").drop(
    columns=["category"]
)
products = products.merge(product_sizes, on="size", how="left").drop(columns=["size"])
products = products.merge(product_colors, on="color", how="left").drop(
    columns=["color"]
)
products = products.merge(product_brands, on="brand", how="left").drop(
    columns=["brand"]
)
products = products.merge(product_materials, on="material", how="left").drop(
    columns=["material"]
)
products["id"] = products.index + 1
products = products.drop_duplicates()

sales = df.loc[
    :, df.columns.isin([c for c in df.columns if "sale" in c.lower()] + ["id"])
]


spark_psql_session = (
    SparkSession.builder.appName("WriteToPostgres")
    .config("spark.jars", "/jdbc-drivers/postgresql-42.7.5.jar")
    .getOrCreate()
)

data = [
    ("countries", spark_psql_session.createDataFrame(countries)),
    ("cities", spark_psql_session.createDataFrame(cities)),
    ("stores", spark_psql_session.createDataFrame(stores)),
    ("pet_types", spark_psql_session.createDataFrame(pet_types)),
    ("pet_breeds", spark_psql_session.createDataFrame(pet_breeds)),
    ("pet_categories", spark_psql_session.createDataFrame(pet_categories)),
    ("customers", spark_psql_session.createDataFrame(customers)),
    ("sellers", spark_psql_session.createDataFrame(sellers)),
    ("suppliers", spark_psql_session.createDataFrame(suppliers)),
    ("product_names", spark_psql_session.createDataFrame(product_names)),
    ("product_categories", spark_psql_session.createDataFrame(product_categories)),
    ("product_sizes", spark_psql_session.createDataFrame(product_sizes)),
    ("product_colors", spark_psql_session.createDataFrame(product_colors)),
    ("product_brands", spark_psql_session.createDataFrame(product_brands)),
    ("product_materials", spark_psql_session.createDataFrame(product_materials)),
    ("products", spark_psql_session.createDataFrame(products)),
    ("sales", spark_psql_session.createDataFrame(sales)),
]

url, opt = (
    "jdbc:postgresql://postgres:5432/lab2",
    {"user": "spark", "password": "bomba", "driver": "org.postgresql.Driver"},
)

for table, df in data:
    df.write.jdbc(url=url, table=table, mode="append", properties=opt)

spark_psql_session.stop()
