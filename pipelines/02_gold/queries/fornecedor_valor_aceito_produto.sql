SELECT supplier_id, model_id, brand_name, model_name, valor_negociado_aceito
FROM workspace.gold_core.kpi_fornecedor_valor_aceito_produto
ORDER BY valor_negociado_aceito DESC NULLS LAST;
