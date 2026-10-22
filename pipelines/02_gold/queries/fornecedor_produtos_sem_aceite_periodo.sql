-- Databricks SQL named parameters: :data_inicio and :data_fim (exclusive).
-- Query a supplier-scoped connection/policy; do not rely on a dashboard filter
-- as the only authorization boundary.
SELECT o.supplier_id, o.offer_id, o.model_id, o.brand_name, o.model_name
FROM workspace.gold_core.kpi_fornecedor_ofertas_detalhe o
WHERE NOT EXISTS (
    SELECT 1
    FROM workspace.gold_core.kpi_fornecedor_ofertas_aceitas_data a
    WHERE a.supplier_id = o.supplier_id
      AND a.offer_id = o.offer_id
      AND a.data_criacao_proposta_proxy >= :data_inicio
      AND a.data_criacao_proposta_proxy < :data_fim
);
