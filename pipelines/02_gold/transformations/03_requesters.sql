CREATE OR REFRESH MATERIALIZED VIEW kpi_demandante_resumo AS
WITH unidades AS (
    SELECT
        requester_id,
        COUNT(DISTINCT local_unit_id) AS unidades_cadastradas
    FROM workspace.silver_core.local_units
    GROUP BY requester_id
),
propostas AS (
    SELECT
        requester_id,
        COUNT(DISTINCT proposal_id) AS propostas_cadastradas
    FROM workspace.silver_core.proposals
    GROUP BY requester_id
),
projetos AS (
    SELECT
        requester_id,
        COUNT(DISTINCT project_id) AS projetos_cadastrados
    FROM workspace.silver_core.technical_projects
    GROUP BY requester_id
)
SELECT
    r.requester_id,
    r.company_id,
    COALESCE(u.unidades_cadastradas, 0) AS unidades_cadastradas,
    COALESCE(p.propostas_cadastradas, 0) AS propostas_cadastradas,
    COALESCE(t.projetos_cadastrados, 0) AS projetos_cadastrados
FROM workspace.silver_core.requesters r
LEFT JOIN unidades u ON u.requester_id = r.requester_id
LEFT JOIN propostas p ON p.requester_id = r.requester_id
LEFT JOIN projetos t ON t.requester_id = r.requester_id;

CREATE OR REFRESH MATERIALIZED VIEW kpi_demandante_propostas_status AS
SELECT
    requester_id,
    proposal_status,
    COUNT(DISTINCT proposal_id) AS total_propostas
FROM workspace.silver_core.proposals
WHERE requester_id IS NOT NULL
GROUP BY requester_id, proposal_status;

CREATE OR REFRESH MATERIALIZED VIEW kpi_demandante_projetos_status AS
SELECT
    requester_id,
    project_status,
    COUNT(DISTINCT project_id) AS total_projetos
FROM workspace.silver_core.technical_projects
WHERE requester_id IS NOT NULL
GROUP BY requester_id, project_status;