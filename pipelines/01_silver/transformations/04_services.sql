CREATE OR REFRESH MATERIALIZED VIEW local_units AS
SELECT
    id AS local_unit_id,
    fk_requester AS requester_id,
    fk_address AS address_id,
    UPPER(TRIM(location_type)) AS location_type
FROM workspace.bronze_core.local_unit;

CREATE OR REFRESH MATERIALIZED VIEW technical_projects AS
SELECT
    id AS project_id,
    fk_requester AS requester_id,
    fk_local_unit AS local_unit_id,
    owner_company_id,
    owner_user_id,
    NULLIF(TRIM(name), '') AS project_name,
    UPPER(TRIM(status)) AS project_status,
    archived,
    created_at,
    start_date,
    end_date,
    UPPER(TRIM(location_state)) AS location_state,
    NULLIF(TRIM(location_city), '') AS location_city
FROM workspace.bronze_core.technical_project;

CREATE OR REFRESH MATERIALIZED VIEW project_activities AS
SELECT
    id AS activity_id,
    project_id,
    technical_service_id AS service_id,
    UPPER(TRIM(`type`)) AS activity_type,
    UPPER(TRIM(status)) AS activity_status,
    starts_at,
    ends_at,
    onsite
FROM workspace.bronze_core.project_activity;

CREATE OR REFRESH MATERIALIZED VIEW technical_services AS
SELECT
    id AS service_id,
    fk_technical_project AS project_id,
    UPPER(TRIM(status)) AS service_status,
    scheduled_date,
    created_at,
    accepted_at,
    end_date
FROM workspace.bronze_core.technical_service;

CREATE OR REFRESH MATERIALIZED VIEW service_contracts AS
SELECT
    id AS contract_id,
    fk_service AS service_id,
    delivery_deadline,
    insurance,
    utility_approval
FROM workspace.bronze_core.service_contract;

CREATE OR REFRESH MATERIALIZED VIEW service_executors AS
SELECT
    id AS executor_id,
    fk_service AS service_id,
    fk_technician_affiliation AS affiliation_id,
    NULLIF(TRIM(`function`), '') AS executor_function
FROM workspace.bronze_core.service_executor;

CREATE OR REFRESH MATERIALIZED VIEW professional_reviews AS
SELECT
    id AS review_id,
    fk_professional AS technician_id,
    fk_service AS service_id,
    rating,
    active,
    created_at
FROM workspace.bronze_core.professional_review;