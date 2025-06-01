#!/bin/bash

echo "Checking ClickHouse Analytics Marts..."
echo "======================================"

run_query() {
    local query=$1
    local description=$2
    echo -e "\n$description:"
    echo "----------------------------------------"
    docker exec bigdataspark-clickhouse-1 clickhouse-client --query "$query" --format Pretty
}

run_query "SHOW TABLES" \
    "Available Tables"

run_query "SELECT name, category, total_quantity_sold, round(total_revenue, 2) as revenue, round(avg_rating, 2) as rating 
          FROM top_products 
          ORDER BY total_revenue DESC 
          LIMIT 5" \
    "Top 5 Products by Revenue"

run_query "SELECT category, round(category_revenue, 2) as revenue, round(category_avg_rating, 2) as avg_rating 
          FROM category_revenue 
          ORDER BY revenue DESC 
          LIMIT 5" \
    "Top 5 Categories by Revenue"

run_query "SELECT customer_first_name, customer_last_name, round(total_spent, 2) as total_spent, purchase_count as total_orders 
          FROM top_customers 
          ORDER BY total_spent DESC 
          LIMIT 5" \
    "Top 5 Customers by Spending"

run_query "SELECT country, round(location_revenue, 2) as revenue, location_orders as total_orders 
          FROM store_geo_distribution 
          ORDER BY revenue DESC 
          LIMIT 5" \
    "Top 5 Countries by Revenue"

run_query "SELECT year, month, round(monthly_revenue, 2) as revenue, order_count as total_orders 
          FROM time_analysis 
          ORDER BY year DESC, month DESC 
          LIMIT 5" \
    "Recent Monthly Sales Trends"

run_query "SELECT round(rating_sales_correlation, 3) as correlation 
          FROM rating_sales_correlation 
          LIMIT 1" \
    "Overall Rating-Sales Correlation"

run_query "SELECT 
            table,
            formatReadableSize(total_bytes) as size,
            total_rows as rows
          FROM system.tables
          WHERE database = 'default'
          ORDER BY total_bytes DESC" \
    "Table Statistics" 