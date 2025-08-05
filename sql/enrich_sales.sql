USE dwh;

INSERT INTO fact_sales (
    receipt_id, store_id, date_id, sku_id, brand, quantity, price, discount,
    cost_price, compensation_percent, revenue, cost, compensation, profit, profit_margin
)
SELECT
    s.receipt_id,
    s.store_id,
    DATE(s.datetime) AS date_id,
    s.sku_id,
    s.brand,
    s.quantity,
    s.price,
    COALESCE(
        s.discount,
        (SELECT p.discount_value
         FROM promotions p
         WHERE p.sku_id = s.sku_id
         AND p.promotion_id = s.promotion_id
         AND s.datetime BETWEEN p.start_date AND p.end_date
         LIMIT 1),
        0.0
    ) AS discount,
    (SELECT sc.cost_price
     FROM sku_costs sc
     WHERE sc.sku_id = s.sku_id
     ORDER BY sc.updated_at DESC
     LIMIT 1) AS cost_price,
    (SELECT sc.compensation_percent
     FROM sku_compensations sc
     WHERE sc.sku_id = s.sku_id
     LIMIT 1) AS compensation_percent,
    (s.price - COALESCE(
        s.discount,
        (SELECT p.discount_value
         FROM promotions p
         WHERE p.sku_id = s.sku_id
         AND p.promotion_id = s.promotion_id
         AND s.datetime BETWEEN p.start_date AND p.end_date
         LIMIT 1),
        0.0
    )) * s.quantity AS revenue,
    (SELECT sc.cost_price
     FROM sku_costs sc
     WHERE sc.sku_id = s.sku_id
     ORDER BY sc.updated_at DESC
     LIMIT 1) * s.quantity AS cost,
    (SELECT sc.compensation_percent
     FROM sku_compensations sc
     WHERE sc.sku_id = s.sku_id
     LIMIT 1) / 100.0 *
    (SELECT sc.cost_price
     FROM sku_costs sc
     WHERE sc.sku_id = s.sku_id
     ORDER BY sc.updated_at DESC
     LIMIT 1) * s.quantity AS compensation,
    ((s.price - COALESCE(
        s.discount,
        (SELECT p.discount_value
         FROM promotions p
         WHERE p.sku_id = s.sku_id
         AND p.promotion_id = s.promotion_id
         AND s.datetime BETWEEN p.start_date AND p.end_date
         LIMIT 1),
        0.0
    )) * s.quantity) -
    ((SELECT sc.cost_price
      FROM sku_costs sc
      WHERE sc.sku_id = s.sku_id
      ORDER BY sc.updated_at DESC
      LIMIT 1) * s.quantity) +
    ((SELECT sc.compensation_percent
      FROM sku_compensations sc
      WHERE sc.sku_id = s.sku_id
      LIMIT 1) / 100.0 *
     (SELECT sc.cost_price
      FROM sku_costs sc
      WHERE sc.sku_id = s.sku_id
      ORDER BY sc.updated_at DESC
      LIMIT 1) * s.quantity) AS profit,
    CASE
        WHEN ((s.price - COALESCE(
            s.discount,
            (SELECT p.discount_value
             FROM promotions p
             WHERE p.sku_id = s.sku_id
             AND p.promotion_id = s.promotion_id
             AND s.datetime BETWEEN p.start_date AND p.end_date
             LIMIT 1),
            0.0
        )) * s.quantity) = 0 THEN 0
        ELSE (((s.price - COALESCE(
            s.discount,
            (SELECT p.discount_value
             FROM promotions p
             WHERE p.sku_id = s.sku_id
             AND p.promotion_id = s.promotion_id
             AND s.datetime BETWEEN p.start_date AND p.end_date
             LIMIT 1),
            0.0
        )) * s.quantity) -
        ((SELECT sc.cost_price
          FROM sku_costs sc
          WHERE sc.sku_id = s.sku_id
          ORDER BY sc.updated_at DESC
          LIMIT 1) * s.quantity) +
        ((SELECT sc.compensation_percent
          FROM sku_compensations sc
          WHERE sc.sku_id = s.sku_id
          LIMIT 1) / 100.0 *
         (SELECT sc.cost_price
          FROM sku_costs sc
          WHERE sc.sku_id = s.sku_id
          ORDER BY sc.updated_at DESC
          LIMIT 1) * s.quantity)) /
        ((s.price - COALESCE(
            s.discount,
            (SELECT p.discount_value
             FROM promotions p
             WHERE p.sku_id = s.sku_id
             AND p.promotion_id = s.promotion_id
             AND s.datetime BETWEEN p.start_date AND p.end_date
             LIMIT 1),
            0.0
        )) * s.quantity) END AS profit_margin
FROM staging_receipts s
WHERE NOT EXISTS (
    SELECT 1 FROM fact_sales f
    WHERE f.receipt_id = s.receipt_id AND f.sku_id = s.sku_id
)
AND DATE(s.datetime) = CURRENT_DATE;

DELETE FROM staging_receipts WHERE DATE(datetime) = CURRENT_DATE;