-- Weekly revenue
SELECT
  week_start,
  total_revenue,
  total_invoices,
  total_units
FROM online_retail_capstone.gold_weekly_revenue
ORDER BY week_start;

-- Top 5 selling products by revenue
SELECT
  stock_code,
  description,
  total_revenue,
  total_units
FROM online_retail_capstone.gold_top_products
ORDER BY total_revenue DESC;

-- Top 5 customers by spend
SELECT
  customer_id,
  total_revenue,
  total_invoices,
  total_units
FROM online_retail_capstone.gold_top_customers
ORDER BY total_revenue DESC;

-- Total revenue by country
SELECT
  country,
  total_revenue,
  total_invoices,
  total_units
FROM online_retail_capstone.gold_revenue_by_country
ORDER BY total_revenue DESC;

