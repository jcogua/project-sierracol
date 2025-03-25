SELECT * FROM public.energy_metrics where country = 'Colombia' order by year asc

--1. Total de Consumo Energético por Año
SELECT 
    year, 
    SUM(primary_energy_consumption) AS total_energy_consumption
FROM 
    public.energy_metrics
WHERE 
    year >= 2000
GROUP BY 
    year;

--2. Top 10 Países por Producción Energética en un Año Específico

SELECT 
    country, 
    SUM(coal_production) + SUM(gas_production) + SUM(oil_production) as energy_production
FROM 
    public.energy_metrics
WHERE 
    year = 2023
	AND ISO_CODE IS NOT NULL
GROUP BY 
    country
ORDER BY 2 DESC

--3. Estadísticas Básicas de Consumo Energético por País

SELECT 
    country,
    AVG(primary_energy_consumption) AS avg_consumption,
    MIN(primary_energy_consumption) AS min_consumption,
    MAX(primary_energy_consumption) AS max_consumption
FROM 
    public.energy_metrics
GROUP BY 
    country;

--4. Análisis Integral del Crecimiento del Consumo y Producción Energética por País (desde el 2000)

WITH yearly_stats AS (
	-- consumo y producción energética por país y año.
    SELECT
        country,
        year,
        SUM(primary_energy_consumption) AS total_consumption,
        SUM(coal_production) + SUM(gas_production) + SUM(oil_production) AS total_production
    FROM
        public.energy_metrics
    WHERE
        year >= 2000
    GROUP BY
        country, year
),
growth_rates AS (
	-- tasa de crecimiento del consumo energético año a año para cada país
    SELECT
        country,
        year,
        total_consumption,
        total_production,
        LAG(total_consumption) OVER (PARTITION BY country ORDER BY year) AS prev_consumption,
        CASE 
            WHEN LAG(total_consumption) OVER (PARTITION BY country ORDER BY year) IS NULL THEN NULL
            ELSE (total_consumption - LAG(total_consumption) OVER (PARTITION BY country ORDER BY year)) 
                 / NULLIF(LAG(total_consumption) OVER (PARTITION BY country ORDER BY year), 0)
        END AS consumption_growth
    FROM
        yearly_stats
),
average_growth AS (
    -- crecimiento promedio anual del consumo para cada país
    SELECT
        country,
        AVG(consumption_growth) AS avg_consumption_growth
    FROM
        growth_rates
    GROUP BY
        country
)
SELECT
    gr.country,
    gr.year,
    gr.total_consumption,
    gr.total_production,
    gr.consumption_growth,
    ag.avg_consumption_growth
FROM
    growth_rates gr
LEFT JOIN
    average_growth ag ON gr.country = ag.country
ORDER BY
    gr.country, gr.year;


	
