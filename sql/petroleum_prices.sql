SELECT area_name FROM public.petroleum_prices

-- Agrupacion de datos diarios en niveles mensuales, calculando promedios, totales, minimos y maximos

SELECT 
    DATE_TRUNC('month', date) AS month,
    AVG(price_usd_per_unit) AS avg_value,
    SUM(price_usd_per_unit) AS total_value,
    MIN(price_usd_per_unit) AS min_value,
    MAX(price_usd_per_unit) AS max_value
FROM 
    public.petroleum_prices
WHERE 
    date >= '2023-01-01'
GROUP BY 
    DATE_TRUNC('month', date)
ORDER BY 
    month;


-- Variación porcentual diaria del valor

WITH daily_aggregates AS (
    SELECT 
        product_name,
        process_name,
        date,
        SUM(price_usd_per_unit) AS daily_value
    FROM 
        public.petroleum_prices
    WHERE 
        date >= '2023-01-01'
    GROUP BY 
        product_name, process_name, date
)
SELECT
    product_name,
    process_name,
    date,
    daily_value,
    LAG(daily_value) OVER (PARTITION BY product_name, process_name ORDER BY date) AS prev_value,
    (daily_value - LAG(daily_value) OVER (PARTITION BY product_name, process_name ORDER BY date)) / 
        NULLIF(LAG(daily_value) OVER (PARTITION BY product_name, process_name ORDER BY date), 0) AS percent_change
FROM 
    daily_aggregates
ORDER BY 
    product_name, process_name, date;

-- Rank de precios mas altos x producto y fecha

WITH daily_aggregates AS (
    SELECT 
        product_name,
        date,
        SUM(price_usd_per_unit) AS daily_value
    FROM 
        public.petroleum_prices
    WHERE 
        date >= '2023-01-01'
    GROUP BY 
        product_name, date
)
SELECT
    product_name,
    date,
    daily_value,
    RANK() OVER (PARTITION BY product_name ORDER BY daily_value DESC) AS value_rank
FROM 
    daily_aggregates
ORDER BY 
    product_name, value_rank;


