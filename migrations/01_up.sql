-- public.cities definition

-- Drop table

-- DROP TABLE public.cities;

CREATE TABLE public.cities (
	city varchar(50) NULL,
	city_id int4 NOT NULL,
	CONSTRAINT pk_cities PRIMARY KEY (city_id)
);


-- public.countries definition

-- Drop table

-- DROP TABLE public.countries;

CREATE TABLE public.countries (
	country varchar(50) NULL,
	country_id int4 NOT NULL,
	CONSTRAINT pk_countries PRIMARY KEY (country_id)
);


-- public.pet_breeds definition

-- Drop table

-- DROP TABLE public.pet_breeds;

CREATE TABLE public.pet_breeds (
	breed varchar(50) NULL,
	pet_breed_id int4 NOT NULL,
	CONSTRAINT pk_pet_breeds PRIMARY KEY (pet_breed_id)
);


-- public.pet_categories definition

-- Drop table

-- DROP TABLE public.pet_categories;

CREATE TABLE public.pet_categories (
	category varchar(50) NULL,
	pet_category_id int4 NOT NULL,
	CONSTRAINT pk_pet_categories PRIMARY KEY (pet_category_id)
);


-- public.pet_types definition

-- Drop table

-- DROP TABLE public.pet_types;

CREATE TABLE public.pet_types (
	"type" varchar(50) NULL,
	pet_type_id int4 NOT NULL,
	CONSTRAINT pk_pet_types PRIMARY KEY (pet_type_id)
);


-- public.product_brands definition

-- Drop table

-- DROP TABLE public.product_brands;

CREATE TABLE public.product_brands (
	brand varchar(50) NULL,
	product_brand_id int4 NOT NULL,
	CONSTRAINT pk_product_brands PRIMARY KEY (product_brand_id)
);


-- public.product_categories definition

-- Drop table

-- DROP TABLE public.product_categories;

CREATE TABLE public.product_categories (
	category varchar(50) NULL,
	product_category_id int4 NOT NULL,
	CONSTRAINT pk_product_categories PRIMARY KEY (product_category_id)
);


-- public.product_colors definition

-- Drop table

-- DROP TABLE public.product_colors;

CREATE TABLE public.product_colors (
	color varchar(50) NULL,
	product_color_id int4 NOT NULL,
	CONSTRAINT pk_product_colors PRIMARY KEY (product_color_id)
);


-- public.product_materials definition

-- Drop table

-- DROP TABLE public.product_materials;

CREATE TABLE public.product_materials (
	material varchar(50) NULL,
	product_material_id int4 NOT NULL,
	CONSTRAINT pk_product_materials PRIMARY KEY (product_material_id)
);


-- public.product_names definition

-- Drop table

-- DROP TABLE public.product_names;

CREATE TABLE public.product_names (
	"name" varchar(50) NULL,
	product_name_id int4 NOT NULL,
	CONSTRAINT pk_product_names PRIMARY KEY (product_name_id)
);


-- public.product_sizes definition

-- Drop table

-- DROP TABLE public.product_sizes;

CREATE TABLE public.product_sizes (
	"size" varchar(50) NULL,
	product_size_id int4 NOT NULL,
	CONSTRAINT pk_product_sizes PRIMARY KEY (product_size_id)
);


-- public.customers definition

-- Drop table

-- DROP TABLE public.customers;

CREATE TABLE public.customers (
	first_name varchar(50) NULL,
	last_name varchar(50) NULL,
	age int4 NULL,
	email varchar(50) NULL,
	postal_code varchar(50) NULL,
	pet_name varchar(50) NULL,
	id int4 NOT NULL,
	pet_type_id int4 NULL,
	pet_breed_id int4 NULL,
	pet_category_id int4 NULL,
	country_id int4 NULL,
	CONSTRAINT pk_customers PRIMARY KEY (id),
	CONSTRAINT fk_customers_country FOREIGN KEY (country_id) REFERENCES public.countries(country_id),
	CONSTRAINT fk_customers_pet_breed FOREIGN KEY (pet_breed_id) REFERENCES public.pet_breeds(pet_breed_id),
	CONSTRAINT fk_customers_pet_category FOREIGN KEY (pet_category_id) REFERENCES public.pet_categories(pet_category_id),
	CONSTRAINT fk_customers_pet_type FOREIGN KEY (pet_type_id) REFERENCES public.pet_types(pet_type_id)
);


-- public.products definition

-- Drop table

-- DROP TABLE public.products;

CREATE TABLE public.products (
	price float4 NULL,
	quantity int4 NULL,
	product_id int4 NULL,
	weight float4 NULL,
	description varchar(1024) NULL,
	rating float4 NULL,
	reviews int4 NULL,
	release_date varchar(50) NULL,
	expiry_date varchar(50) NULL,
	product_name_id int4 NULL,
	product_category_id int4 NULL,
	product_size_id int4 NULL,
	product_color_id int4 NULL,
	product_brand_id int4 NULL,
	product_material_id int4 NULL,
	id int4 NOT NULL,
	CONSTRAINT pk_products PRIMARY KEY (id),
	CONSTRAINT fk_products_brand FOREIGN KEY (product_brand_id) REFERENCES public.product_brands(product_brand_id),
	CONSTRAINT fk_products_category FOREIGN KEY (product_category_id) REFERENCES public.product_categories(product_category_id),
	CONSTRAINT fk_products_color FOREIGN KEY (product_color_id) REFERENCES public.product_colors(product_color_id),
	CONSTRAINT fk_products_material FOREIGN KEY (product_material_id) REFERENCES public.product_materials(product_material_id),
	CONSTRAINT fk_products_name FOREIGN KEY (product_name_id) REFERENCES public.product_names(product_name_id),
	CONSTRAINT fk_products_size FOREIGN KEY (product_size_id) REFERENCES public.product_sizes(product_size_id)
);


-- public.sellers definition

-- Drop table

-- DROP TABLE public.sellers;

CREATE TABLE public.sellers (
	first_name varchar(50) NULL,
	last_name varchar(50) NULL,
	email varchar(50) NULL,
	postal_code varchar(50) NULL,
	id int4 NOT NULL,
	country_id int4 NULL,
	CONSTRAINT pk_sellers PRIMARY KEY (id),
	CONSTRAINT fk_sellers_country FOREIGN KEY (country_id) REFERENCES public.countries(country_id)
);


-- public.stores definition

-- Drop table

-- DROP TABLE public.stores;

CREATE TABLE public.stores (
	"name" varchar(50) NULL,
	"location" varchar(50) NULL,
	state varchar(50) NULL,
	phone varchar(50) NULL,
	email varchar(50) NULL,
	country_id int4 NULL,
	city_id int4 NULL,
	store_id int4 NOT NULL,
	CONSTRAINT pk_stores PRIMARY KEY (store_id),
	CONSTRAINT fk_stores_city FOREIGN KEY (city_id) REFERENCES public.cities(city_id),
	CONSTRAINT fk_stores_country FOREIGN KEY (country_id) REFERENCES public.countries(country_id)
);


-- public.suppliers definition

-- Drop table

-- DROP TABLE public.suppliers;

CREATE TABLE public.suppliers (
	"name" varchar(50) NULL,
	contact varchar(50) NULL,
	email varchar(50) NULL,
	phone varchar(50) NULL,
	address varchar(50) NULL,
	country_id int4 NULL,
	city_id int4 NULL,
	id int4 NOT NULL,
	CONSTRAINT pk_suppliers PRIMARY KEY (id),
	CONSTRAINT fk_suppliers_country FOREIGN KEY (country_id) REFERENCES public.countries(country_id)
);


-- public.sales definition

-- Drop table

-- DROP TABLE public.sales;

CREATE TABLE public.sales (
	id int4 NOT NULL,
	sale_date varchar(50) NULL,
	sale_customer_id int4 NULL,
	sale_seller_id int4 NULL,
	sale_product_id int4 NULL,
	sale_quantity int4 NULL,
	sale_total_price float4 NULL,
	CONSTRAINT pk_sales PRIMARY KEY (id),
	CONSTRAINT fk_sc FOREIGN KEY (sale_customer_id) REFERENCES public.customers(id),
	CONSTRAINT fk_sp FOREIGN KEY (sale_product_id) REFERENCES public.products(id),
	CONSTRAINT fk_ss FOREIGN KEY (sale_seller_id) REFERENCES public.sellers(id)
);


CREATE TABLE public.wdt (
    id BIGINT,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INT,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC,
    product_quantity INT,
    sale_date DATE,
    sale_customer_id BIGINT,
    sale_seller_id BIGINT,
    sale_product_id BIGINT,
    sale_quantity INT,
    sale_total_price NUMERIC,
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT,
    pet_category TEXT,
    product_weight NUMERIC,
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC,
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);