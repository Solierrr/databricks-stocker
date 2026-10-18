CREATE OR REFRESH MATERIALIZED VIEW kpi_admin_cadastros AS
WITH fornecedores AS (
    SELECT COUNT(DISTINCT supplier_id) AS fornecedores_ativos
    FROM workspace.silver_core.suppliers
    WHERE supplier_status = 'ACTIVE'
),
demandantes AS (
    SELECT COUNT(DISTINCT requester_id) AS demandantes_cadastrados
    FROM workspace.silver_core.requesters
)
SELECT
    fornecedores.fornecedores_ativos,
    demandantes.demandantes_cadastrados
FROM fornecedores
CROSS JOIN demandantes;

CREATE OR REFRESH MATERIALIZED VIEW kpi_admin_modelos_status AS
SELECT
    model_status,
    COUNT(DISTINCT model_id) AS total_modelos
FROM workspace.silver_core.product_models
GROUP BY model_status;

CREATE OR REFRESH MATERIALIZED VIEW kpi_admin_propostas_status AS
SELECT
    proposal_status,
    COUNT(DISTINCT proposal_id) AS total_propostas
FROM workspace.silver_core.proposals
GROUP BY proposal_status;