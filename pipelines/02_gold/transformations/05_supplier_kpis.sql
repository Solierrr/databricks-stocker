-- Medidas do dashboard do fornecedor. Toda linha conserva supplier_id para
-- permitir isolamento na camada de consumo; o exportador aplica RLS no destino.
CREATE OR REFRESH MATERIALIZED VIEW kpi_fornecedor_ofertas_resumo
WITH ROW FILTER workspace.gold_security.can_read_supplier ON (supplier_id) AS
WITH referencia AS (SELECT current_date() AS data_referencia)
SELECT
    s.supplier_id,
    r.data_referencia,
    COUNT(DISTINCT o.offer_id) AS ofertas_cadastradas,
    COUNT(DISTINCT CASE WHEN o.commercial_status = 'ACTIVE'
        AND (o.expiration_date IS NULL OR to_date(o.expiration_date) >= r.data_referencia)
        THEN o.offer_id END) AS ofertas_ativas,
    AVG(o.unit_price) AS preco_medio_anunciado,
    AVG(CASE WHEN o.commercial_status = 'ACTIVE'
        AND (o.expiration_date IS NULL OR to_date(o.expiration_date) >= r.data_referencia)
        THEN o.unit_price END) AS preco_medio_anunciado_ativo,
    SUM(o.availability) AS disponibilidade_anunciada,
    COUNT(DISTINCT CASE WHEN to_date(o.expiration_date) < r.data_referencia
        THEN o.offer_id END) AS ofertas_expiradas,
    COUNT(DISTINCT CASE WHEN to_date(o.expiration_date) >= r.data_referencia
        AND to_date(o.expiration_date) < date_add(r.data_referencia, 30)
        THEN o.offer_id END) AS ofertas_vencendo_30_dias
FROM workspace.silver_core.suppliers s
CROSS JOIN referencia r
LEFT JOIN workspace.silver_core.offers o ON o.supplier_id = s.supplier_id
GROUP BY s.supplier_id, r.data_referencia;

CREATE OR REFRESH MATERIALIZED VIEW kpi_fornecedor_ofertas_detalhe
WITH ROW FILTER workspace.gold_security.can_read_supplier ON (supplier_id) AS
SELECT
    o.supplier_id,
    o.offer_id,
    o.model_id,
    b.brand_name,
    m.model_name,
    o.unit_price,
    o.availability,
    o.expiration_date,
    o.commercial_status
FROM workspace.silver_core.offers o
LEFT JOIN workspace.silver_core.product_models m ON m.model_id = o.model_id
LEFT JOIN workspace.silver_core.brands b ON b.brand_id = m.brand_id;

CREATE OR REFRESH MATERIALIZED VIEW kpi_fornecedor_estoque_resumo
WITH ROW FILTER workspace.gold_security.can_read_supplier ON (supplier_id) AS
WITH estoque AS (
    SELECT supplier_id, model_id, SUM(quantity) AS unidades_estoque
    FROM workspace.silver_core.inventory_items GROUP BY supplier_id, model_id
)
SELECT
    s.supplier_id,
    SUM(e.unidades_estoque) AS unidades_estoque,
    COUNT(DISTINCT CASE WHEN e.unidades_estoque > 0 THEN e.model_id END) AS produtos_com_estoque
FROM workspace.silver_core.suppliers s
LEFT JOIN estoque e
    ON e.supplier_id = s.supplier_id
GROUP BY s.supplier_id;

CREATE OR REFRESH MATERIALIZED VIEW kpi_fornecedor_propostas_status
WITH ROW FILTER workspace.gold_security.can_read_supplier ON (supplier_id) AS
SELECT
    o.supplier_id,
    p.proposal_status,
    COUNT(DISTINCT p.proposal_id) AS propostas_recebidas
FROM workspace.silver_core.proposal_items i
JOIN workspace.silver_core.offers o ON o.offer_id = i.offer_id
JOIN workspace.silver_core.proposals p ON p.proposal_id = i.proposal_id
GROUP BY o.supplier_id, p.proposal_status;

CREATE OR REFRESH MATERIALIZED VIEW kpi_fornecedor_propostas_resumo
WITH ROW FILTER workspace.gold_security.can_read_supplier ON (supplier_id) AS
WITH propostas_fornecedor AS (
    SELECT DISTINCT o.supplier_id, p.proposal_id, p.proposal_status
    FROM workspace.silver_core.proposal_items i
    JOIN workspace.silver_core.offers o ON o.offer_id = i.offer_id
    JOIN workspace.silver_core.proposals p ON p.proposal_id = i.proposal_id
),
propostas AS (
    SELECT supplier_id,
        COUNT(DISTINCT CASE WHEN proposal_status IN ('AWAITING_SUPPLIER', 'AWAITING_REQUESTER')
            THEN proposal_id END) AS propostas_abertas,
        COUNT(DISTINCT CASE WHEN proposal_status = 'ACCEPTED' THEN proposal_id END) AS propostas_aceitas,
        COUNT(DISTINCT CASE WHEN proposal_status IN ('ACCEPTED', 'REJECTED', 'CANCELED')
            THEN proposal_id END) AS propostas_encerradas
    FROM propostas_fornecedor GROUP BY supplier_id
),
itens AS (
    SELECT o.supplier_id,
        COUNT(CASE WHEN p.proposal_status = 'ACCEPTED' THEN 1 END) AS itens_aceitos,
        COUNT(CASE WHEN p.proposal_status = 'ACCEPTED' AND i.negotiated_price IS NULL
            THEN 1 END) AS itens_aceitos_sem_preco,
        COUNT(CASE WHEN p.proposal_status = 'ACCEPTED' AND i.negotiated_price IS NOT NULL
            THEN 1 END) AS itens_aceitos_com_preco,
        SUM(CASE WHEN p.proposal_status = 'ACCEPTED'
            THEN i.negotiated_price * i.quantity END) AS valor_negociado_aceito
    FROM workspace.silver_core.proposal_items i
    JOIN workspace.silver_core.offers o ON o.offer_id = i.offer_id
    JOIN workspace.silver_core.proposals p ON p.proposal_id = i.proposal_id
    GROUP BY o.supplier_id
)
SELECT s.supplier_id,
    COALESCE(p.propostas_abertas, 0) AS propostas_abertas_recebidas,
    COALESCE(p.propostas_aceitas, 0) AS propostas_aceitas,
    COALESCE(p.propostas_encerradas, 0) AS propostas_encerradas,
    100.0 * p.propostas_aceitas / NULLIF(p.propostas_encerradas, 0)
        AS taxa_aceite_encerradas_percentual,
    COALESCE(i.itens_aceitos_sem_preco, 0) AS itens_aceitos_sem_preco,
    100.0 * i.itens_aceitos_com_preco / NULLIF(i.itens_aceitos, 0)
        AS cobertura_preco_itens_aceitos_percentual,
    i.valor_negociado_aceito
FROM workspace.silver_core.suppliers s
LEFT JOIN propostas p ON p.supplier_id = s.supplier_id
LEFT JOIN itens i ON i.supplier_id = s.supplier_id;

CREATE OR REFRESH MATERIALIZED VIEW kpi_fornecedor_valor_aceito_produto
WITH ROW FILTER workspace.gold_security.can_read_supplier ON (supplier_id) AS
SELECT o.supplier_id, o.model_id, b.brand_name, m.model_name,
    SUM(i.negotiated_price * i.quantity) AS valor_negociado_aceito
FROM workspace.silver_core.proposal_items i
JOIN workspace.silver_core.offers o ON o.offer_id = i.offer_id
JOIN workspace.silver_core.proposals p ON p.proposal_id = i.proposal_id
LEFT JOIN workspace.silver_core.product_models m ON m.model_id = o.model_id
LEFT JOIN workspace.silver_core.brands b ON b.brand_id = m.brand_id
WHERE p.proposal_status = 'ACCEPTED'
GROUP BY o.supplier_id, o.model_id, b.brand_name, m.model_name;

CREATE OR REFRESH MATERIALIZED VIEW kpi_fornecedor_valor_aceito_mes
WITH ROW FILTER workspace.gold_security.can_read_supplier ON (supplier_id) AS
SELECT o.supplier_id,
    date_trunc('MONTH', p.created_at) AS mes_criacao_proposta_proxy,
    SUM(i.negotiated_price * i.quantity) AS valor_negociado_aceito
FROM workspace.silver_core.proposal_items i
JOIN workspace.silver_core.offers o ON o.offer_id = i.offer_id
JOIN workspace.silver_core.proposals p ON p.proposal_id = i.proposal_id
WHERE p.proposal_status = 'ACCEPTED'
GROUP BY o.supplier_id, date_trunc('MONTH', p.created_at);

-- A data exata permite filtrar qualquer intervalo sem perder meses parciais.
CREATE OR REFRESH MATERIALIZED VIEW kpi_fornecedor_ofertas_aceitas_data
WITH ROW FILTER workspace.gold_security.can_read_supplier ON (supplier_id) AS
SELECT o.supplier_id, o.offer_id, o.model_id,
    to_date(p.created_at) AS data_criacao_proposta_proxy,
    COUNT(DISTINCT p.proposal_id) AS propostas_aceitas
FROM workspace.silver_core.proposal_items i
JOIN workspace.silver_core.offers o ON o.offer_id = i.offer_id
JOIN workspace.silver_core.proposals p ON p.proposal_id = i.proposal_id
WHERE p.proposal_status = 'ACCEPTED'
GROUP BY o.supplier_id, o.offer_id, o.model_id, to_date(p.created_at);
