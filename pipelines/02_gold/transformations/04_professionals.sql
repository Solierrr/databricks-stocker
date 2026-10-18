CREATE OR REFRESH MATERIALIZED VIEW kpi_profissional_resumo AS
SELECT
    a.affiliation_id,
    a.technician_id,
    t.user_id,
    a.company_id,
    a.affiliation_type,
    a.active AS affiliation_active,
    COUNT(DISTINCT e.service_id) AS servicos_atribuidos
FROM workspace.silver_core.technician_affiliations a
LEFT JOIN workspace.silver_core.technicians t
    ON t.technician_id = a.technician_id
LEFT JOIN workspace.silver_core.service_executors e
    ON e.affiliation_id = a.affiliation_id
GROUP BY
    a.affiliation_id,
    a.technician_id,
    t.user_id,
    a.company_id,
    a.affiliation_type,
    a.active;

CREATE OR REFRESH MATERIALIZED VIEW kpi_profissional_servicos_status AS
SELECT
    a.affiliation_id,
    a.technician_id,
    a.affiliation_type,
    s.service_status,
    COUNT(DISTINCT e.service_id) AS total_servicos
FROM workspace.silver_core.technician_affiliations a
JOIN workspace.silver_core.service_executors e
    ON e.affiliation_id = a.affiliation_id
JOIN workspace.silver_core.technical_services s
    ON s.service_id = e.service_id
GROUP BY
    a.affiliation_id,
    a.technician_id,
    a.affiliation_type,
    s.service_status;

CREATE OR REFRESH MATERIALIZED VIEW kpi_profissional_avaliacoes AS
SELECT
    technician_id,
    COUNT(DISTINCT review_id) AS total_avaliacoes,
    ROUND(AVG(rating), 2) AS nota_media
FROM workspace.silver_core.professional_reviews
WHERE active = true
GROUP BY technician_id;