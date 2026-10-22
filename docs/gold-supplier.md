# KPIs Gold do fornecedor

As oito tabelas novas são calculadas em Python em
`pipelines/00_medallion/transformations/note_gold.py`. O SQL anterior em
`archive/legacy_pipelines/02_gold/transformations/05_supplier_kpis.sql` foi preservado como
referência de cálculo durante o cutover.
Elas complementam as duas tabelas de fornecedor já existentes em
`02_suppliers.sql`. Todas carregam `supplier_id` e aplicam
`workspace.gold_security.can_read_supplier` como row filter na gravação da
tabela Delta. O filtro e seu mapa de identidades devem ser criados por
`security/supplier_access.sql` antes da atualização da Gold. O mapa começa vazio;
o usuário da pipeline e o usuário que exporta para AnalyticsDB precisam de
vínculo ADMIN validado. Fornecedores recebem vínculo SUPPLIER para um único
`supplier_id`. O acesso físico e as concessões de catálogo precisam ser
configurados para que consumidores não alterem o mapa ou a função.

| Família | Tabelas | Regra |
|---|---|---|
| Ofertas | `kpi_fornecedor_ofertas_resumo`, `kpi_fornecedor_ofertas_detalhe`, `kpi_fornecedor_ofertas_status` | Distintas por oferta; ativa exige status `ACTIVE` e validade não expirada; prazo de 30 dias exclui validade nula. |
| Estoque | `kpi_fornecedor_estoque_resumo`, `kpi_fornecedor_estoque_modelo` | Soma por fornecedor/modelo; ausência de registro permanece distinta de estoque zero. |
| Propostas | `kpi_fornecedor_propostas_status`, `kpi_fornecedor_propostas_resumo` | Fornecedor via item → oferta; propostas distintas por fornecedor; taxa usa apenas encerradas e informa o denominador. |
| Valor aceito | `kpi_fornecedor_valor_aceito_produto`, `kpi_fornecedor_valor_aceito_mes`, `kpi_fornecedor_ofertas_aceitas_data` | Preço negociado × quantidade, sem desconto adicional; preço ausente permanece nulo. Mês e data são da criação da proposta atualmente aceita, não do aceite. |

`archive/legacy_pipelines/02_gold/queries/fornecedor_produtos_sem_aceite_periodo.sql` lista
ofertas sem proposta aceita em qualquer intervalo `[data_inicio, data_fim)`.
Uma oferta pode representar um modelo que também tem outras ofertas; para
uma lista única de modelos, agrupe o resultado por `supplier_id, model_id` e
exija ausência de aceite em todas as ofertas do modelo.

`archive/legacy_pipelines/02_gold/queries/fornecedor_produtos_sem_estoque.sql` lista somente
modelos com estoque zero registrado explicitamente.
`archive/legacy_pipelines/02_gold/queries/fornecedor_valor_aceito_produto.sql` apresenta o
valor aceito por produto do maior para o menor.

Os quatro KPIs de pedidos do documento são condicionais. Este checkout não
tem `purchase_order` e `order_item` na Silver/Gold, então não há fonte para
calculá-los com segurança. `database-console/db/core/schema.sql` também não
contém essas entidades. Não os representamos por cobranças de assinatura.

Há duas divergências estáticas com o schema CoreDB versionado:
`offers` na Silver lê `commercial_status`, ausente em `db/core/schema.sql`, e
`proposals` lê `fk_supplier`, também ausente. O cálculo novo usa o
relacionamento item → oferta para atribuir fornecedor, mas a pipeline Silver
existente ainda precisa ser alinhada com a origem efetiva.

O job em `exports/medallion_export_job.json` executa o notebook único de
transformação. Confirme o caminho do notebook no workspace, configure o compute, instale o job e
crie no escopo de segredos Databricks `solaria-sync` as chaves `endpoint-url`
(URL base da API) e `api-token` (o `SYNC_API_TOKEN` já usado pela API). A task
de exportação depende da atualização Bronze → Silver → Gold; uma falha impede a
chamada. O notebook não repete a requisição automaticamente. A API precisa
estar acessível a partir do Databricks.

Nenhuma pipeline, endpoint ou banco foi executado nesta entrega.
