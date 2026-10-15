# Databricks Stocker

Workspace analítico da Solaria responsável pelas camadas Silver e Gold. A ingestão do CoreDB para a Bronze pertence ao serviço databricks-sync.

## Estrutura

- pipelines/01_silver/: transformação da Bronze em dados tratados.
- pipelines/02_gold/: indicadores destinados aos dashboards e ao front.
- exports/: exportação futura da Gold para o AnalyticsDB.
- dashboards/: dashboards reais exportados do Databricks quando forem criados.
- docs/: arquitetura, regras da Silver e configuração manual no Databricks.
- archive/: notebooks e dashboards anteriores, preservados como referência.

Os arquivos novos estão vazios e ainda não representam recursos executáveis.
