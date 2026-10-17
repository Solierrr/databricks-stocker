CREATE OR REFRESH MATERIALIZED VIEW company_plans AS
SELECT
    id AS plan_id,
    NULLIF(TRIM(name), '') AS plan_name,
    value AS plan_value,
    UPPER(TRIM(cycle)) AS billing_cycle
FROM workspace.bronze_core.company_plans;

CREATE OR REFRESH MATERIALIZED VIEW subscriptions AS
SELECT
    id AS subscription_id,
    fk_supplier AS supplier_id,
    fk_plan AS plan_id,
    UPPER(TRIM(status)) AS subscription_status,
    auto_renewal,
    start_date,
    end_date
FROM workspace.bronze_core.subscription;

CREATE OR REFRESH MATERIALIZED VIEW subscription_charges AS
SELECT
    id AS charge_id,
    fk_subscription AS subscription_id,
    amount,
    UPPER(TRIM(payment_method)) AS payment_method,
    UPPER(TRIM(status)) AS charge_status,
    due_date,
    payment_date
FROM workspace.bronze_core.charge;