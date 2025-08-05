USE dwh;

INSERT INTO sales_dashboard_today (
    brand, total_sales, total_discounts, total_cost, total_compensation, total_profit, profit_margin
)
SELECT
    brand,
    SUM(revenue) AS total_sales,
    SUM(discount * quantity) AS total_discounts,
    SUM(cost) AS total_cost,
    SUM(compensation) AS total_compensation,
    SUM(profit) AS total_profit,
    CASE
        WHEN SUM(revenue) = 0 THEN 0
        ELSE SUM(profit) / SUM(revenue)
    END AS profit_margin
FROM fact_sales
WHERE date_id = CURRENT_DATE
GROUP BY brand
ON DUPLICATE KEY UPDATE
    total_sales = VALUES(total_sales),
    total_discounts = VALUES(total_discounts),
    total_cost = VALUES(total_cost),
    total_compensation = VALUES(total_compensation),
    total_profit = VALUES(total_profit),
    profit_margin = VALUES(profit_margin);