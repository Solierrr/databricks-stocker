# Arquitetura de medalhões

## Fluxo

1. O databricks-sync copia os dados do CoreDB para a Bronze.
2. A Pipeline Silver do databricks-stocker limpa e padroniza os dados.
3. A Pipeline Gold calcula indicadores a partir da Silver.
4. Os dashboards do Databricks consultam a Gold.
5. Quando confirmado, um notebook exporta a Gold para o AnalyticsDB.
6. A API consulta o AnalyticsDB e entrega os dados ao front-end.

## Responsabilidades

- databricks-sync: CoreDB → Bronze.
- databricks-stocker: Bronze → Silver → Gold.
- AnalyticsDB: armazenamento dos resultados preparados para a aplicação.
- API: entrega segura dos resultados ao front-end.
