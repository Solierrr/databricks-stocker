# KPIs Gold do fornecedor

As dez tabelas de fornecedor são calculadas em Python em
`pipelines/00_medallion/transformations/note_gold.py`. Todas carregam `supplier_id` e aplicam
`workspace.gold_security.can_read_supplier` como row filter na gravação da
tabela Delta. O filtro e seu mapa de identidades devem ser criados por
`security/supplier_access.sql` antes da atualização da Gold. O mapa começa vazio;
o usuário do job e o usuário que exporta para AnalyticsDB precisam de
vínculo ADMIN validado. Fornecedores recebem vínculo SUPPLIER para um único
`supplier_id`. O acesso físico e as concessões de catálogo precisam ser
configurados para que consumidores não alterem o mapa ou a função.

| Família | Tabelas | Regra |
|---|---|---|
| Ofertas | `kpi_supplier_offers_summary`, `kpi_supplier_offer_details`, `kpi_supplier_offers_by_status` | Distintas por oferta; ativa exige status `ACTIVE` e validade não expirada; prazo de 30 dias exclui validade nula. |
| Estoque | `kpi_supplier_stock_summary`, `kpi_supplier_stock_by_model` | Soma por fornecedor/modelo; ausência de registro permanece distinta de estoque zero. |
| Propostas | `kpi_supplier_proposals_by_status`, `kpi_supplier_proposals_summary` | Fornecedor via item → oferta; propostas distintas por fornecedor; taxa usa apenas encerradas e informa o denominador. |
| Valor aceito | `kpi_supplier_accepted_value_by_model`, `kpi_supplier_accepted_value_by_month`, `kpi_supplier_accepted_offers_by_date` | Preço negociado × quantidade, sem desconto adicional; preço ausente permanece nulo. Mês e data são da criação da proposta atualmente aceita, não do aceite. |

Consultas sob demanda para ofertas sem aceite por intervalo, modelos com
estoque zero registrado e ordenação de valor aceito por produto não fazem
parte do job atual. Se forem necessárias para dashboards, devem ser criadas
separadamente a partir das tabelas Gold, preservando essas regras de negócio.

Os quatro KPIs de pedidos do documento são condicionais. Este checkout não
tem `purchase_order` e `order_item` na Silver/Gold, então não há fonte para
calculá-los com segurança. `database-console/db/core/schema.sql` também não
contém essas entidades. Não os representamos por cobranças de assinatura.

Há duas divergências estáticas com o schema CoreDB versionado:
`offers` na Silver lê `commercial_status`, ausente em `db/core/schema.sql`, e
`proposals` lê `supplier_id`, que o DDL anterior também não tinha. O cálculo novo usa o
relacionamento item → oferta para atribuir fornecedor, mas a pipeline Silver
definida em `note_silver.py` ainda precisa ser alinhada com a origem efetiva.

O job em `exports/medallion_export_job.json` executa o notebook único de
transformação. Confirme o caminho do notebook no workspace, configure o compute, instale o job e
crie no escopo de segredos Databricks `solaria-sync` as chaves `endpoint-url`
(URL base da API) e `api-token` (o `SYNC_API_TOKEN` já usado pela API). A task
de exportação depende da atualização Bronze → Silver → Gold; uma falha impede a
chamada. O notebook não repete a requisição automaticamente. A API precisa
estar acessível a partir do Databricks.

Nenhum job, endpoint ou banco foi executado nesta entrega.
