CREATE OR REFRESH MATERIALIZED VIEW brands AS
SELECT
    id AS brand_id,
    fk_owner_company AS owner_company_id,
    NULLIF(TRIM(name), '') AS brand_name,
    NULLIF(TRIM(normalized_name), '') AS normalized_name,
    UPPER(TRIM(visibility)) AS visibility,
    archived,
    legacy,
    created_at,
    updated_at
FROM workspace.bronze_core.brand;

CREATE OR REFRESH MATERIALIZED VIEW product_models AS
SELECT
    id AS model_id,
    fk_brand AS brand_id,
    NULLIF(TRIM(model), '') AS model_name,
    UPPER(TRIM(status)) AS model_status,
    UPPER(TRIM(product_type)) AS product_type,
    UPPER(TRIM(infrastructure_type)) AS infrastructure_type,
    UPPER(TRIM(`type`)) AS panel_type,
    NULLIF(TRIM(material), '') AS material,
    power_wp,
    efficiency,
    width,
    length,
    weight
FROM workspace.bronze_core.model;

CREATE OR REFRESH MATERIALIZED VIEW offers AS
SELECT
    id AS offer_id,
    fk_supplier AS supplier_id,
    fk_model AS model_id,
    unit_price,
    availability,
    expiration_date,
    NULLIF(TRIM(slug), '') AS slug,
    discount_percentage,
    UPPER(TRIM(commercial_status)) AS commercial_status
FROM workspace.bronze_core.offer;

CREATE OR REFRESH MATERIALIZED VIEW inventory_items AS
SELECT
    id AS inventory_id,
    fk_supplier AS supplier_id,
    fk_model AS model_id,
    quantity
FROM workspace.bronze_core.inventory;