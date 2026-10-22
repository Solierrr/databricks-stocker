-- Apenas estoque zero registrado explicitamente; ausência não é zero.
SELECT supplier_id, model_id, brand_name, model_name
FROM workspace.gold_core.kpi_fornecedor_estoque_modelo
WHERE unidades_estoque = 0;
