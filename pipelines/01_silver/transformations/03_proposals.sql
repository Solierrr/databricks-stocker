CREATE OR REFRESH MATERIALIZED VIEW proposals AS
SELECT
    id AS proposal_id,
    fk_requester AS requester_id,
    fk_supplier AS supplier_id,
    UPPER(TRIM(status)) AS proposal_status,
    total_amount,
    created_at,
    updated_at
FROM workspace.bronze_core.proposal;

CREATE OR REFRESH MATERIALIZED VIEW proposal_items AS
SELECT
    id AS proposal_item_id,
    fk_proposal AS proposal_id,
    fk_offer AS offer_id,
    quantity,
    negotiated_price,
    discount
FROM workspace.bronze_core.proposal_item;

CREATE OR REFRESH MATERIALIZED VIEW proposal_units AS
SELECT
    id AS proposal_unit_id,
    fk_proposal_item AS proposal_item_id,
    fk_local_unit AS local_unit_id,
    quantity
FROM workspace.bronze_core.proposal_unit;