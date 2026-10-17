CREATE OR REFRESH MATERIALIZED VIEW energy_bills AS
SELECT
    id AS energy_bill_id,
    fk_local_unit AS local_unit_id,
    consumption,
    price
FROM workspace.bronze_core.energy_bill;