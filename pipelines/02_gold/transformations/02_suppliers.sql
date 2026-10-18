CREATE OR REFRESH MATERIALIZED VIEW kpi_fornecedor_ofertas_status AS
SELECT
    supplier_id,
    commercial_status,
    COUNT(DISTINCT offer_id) AS total_ofertas
FROM workspace.silver_core.offers
GROUP BY supplier_id, commercial_status;

CREATE OR REFRESH MATERIALIZED VIEW kpi_fornecedor_estoque_modelo AS
WITH estoque AS (
    SELECT
        supplier_id,
        model_id,
        SUM(quantity) AS unidades_estoque
    FROM workspace.silver_core.inventory_items
    GROUP BY supplier_id, model_id
)
SELECT
    e.supplier_id,
    e.model_id,
    m.model_name,
    b.brand_name,
    e.unidades_estoque
FROM estoque e
LEFT JOIN workspace.silver_core.product_models m
    ON m.model_id = e.model_id
LEFT JOIN workspace.silver_core.brands b
    ON b.brand_id = m.brand_id;