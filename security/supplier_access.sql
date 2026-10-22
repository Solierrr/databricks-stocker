-- Provision outside the pipeline, before its first Gold refresh.
-- The mapping starts empty. Add only verified Databricks principals.
CREATE SCHEMA IF NOT EXISTS workspace.gold_security;

CREATE TABLE IF NOT EXISTS workspace.gold_security.supplier_access (
    principal STRING NOT NULL,
    scope STRING NOT NULL,
    supplier_id STRING,
    active BOOLEAN NOT NULL,
    validated_by STRING NOT NULL,
    validated_at TIMESTAMP NOT NULL
) USING DELTA;

CREATE OR REPLACE FUNCTION workspace.gold_security.can_read_supplier(p_supplier_id STRING)
RETURNS BOOLEAN
RETURN EXISTS (
    SELECT 1 FROM workspace.gold_security.supplier_access a
    WHERE a.principal = session_user() AND a.scope = 'ADMIN' AND a.active
)
OR (
    (SELECT COUNT(DISTINCT a.supplier_id)
     FROM workspace.gold_security.supplier_access a
     WHERE a.principal = session_user() AND a.scope = 'SUPPLIER' AND a.active) = 1
    AND EXISTS (
        SELECT 1 FROM workspace.gold_security.supplier_access a
        WHERE a.principal = session_user() AND a.scope = 'SUPPLIER'
          AND a.supplier_id = p_supplier_id AND a.active
    )
);
