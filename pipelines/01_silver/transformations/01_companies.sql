CREATE OR REFRESH MATERIALIZED VIEW companies AS
SELECT
    id AS company_id,
    UPPER(TRIM(`type`)) AS company_type,
    UPPER(TRIM(status)) AS company_status,
    NULLIF(TRIM(trade_name), '') AS trade_name,
    NULLIF(TRIM(corporate_name), '') AS corporate_name,
    NULLIF(TRIM(slug), '') AS slug
FROM workspace.bronze_core.company;

CREATE OR REFRESH MATERIALIZED VIEW suppliers AS
SELECT
    id AS supplier_id,
    fk_company AS company_id,
    UPPER(TRIM(status)) AS supplier_status,
    NULLIF(TRIM(business_type), '') AS business_type
FROM workspace.bronze_core.supplier;

CREATE OR REFRESH MATERIALIZED VIEW requesters AS
SELECT
    id AS requester_id,
    fk_company AS company_id,
    NULLIF(TRIM(business_type), '') AS business_type
FROM workspace.bronze_core.requester;

CREATE OR REFRESH MATERIALIZED VIEW technicians AS
SELECT
    t.id AS technician_id,
    t.fk_person AS person_id,
    p.fk_users AS user_id,
    NULLIF(TRIM(t.slug), '') AS technician_slug
FROM workspace.bronze_core.technician t
LEFT JOIN workspace.bronze_core.person p
    ON p.id = t.fk_person;

CREATE OR REFRESH MATERIALIZED VIEW technician_affiliations AS
SELECT
    id AS affiliation_id,
    fk_technician AS technician_id,
    fk_company AS company_id,
    UPPER(TRIM(affiliation_type)) AS affiliation_type,
    active
FROM workspace.bronze_core.technician_affiliation;

CREATE OR REFRESH MATERIALIZED VIEW company_memberships AS
SELECT
    id AS membership_id,
    fk_company AS company_id,
    fk_users AS user_id,
    fk_position AS position_id
FROM workspace.bronze_core.user_company;