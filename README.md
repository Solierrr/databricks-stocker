# Databricks Stocker

Workspace analítico da Solaria responsável por transformar os dados da camada Bronze nas camadas Silver e Gold. A ingestão do CoreDB para a Bronze pertence ao serviço `databricks-sync`.

## Fluxo de dados

```text
CoreDB
  |
  v
databricks-sync
  |
  v
Bronze
  |
  v
Pipeline Silver
  |
  v
Silver
  |
  v
Pipeline Gold
  |
  v
Gold
  |-- Dashboards do Databricks
  +-- Exportação futura para o AnalyticsDB
```

## Estrutura

- `pipelines/01_silver/`: transformações da Bronze em dados limpos e padronizados.
- `pipelines/02_gold/`: indicadores preparados a partir da Silver.
- `exports/`: notebook reservado para a futura exportação da Gold para o AnalyticsDB.
- `dashboards/`: local destinado aos dashboards exportados do Databricks.
- `docs/architecture.md`: descrição do fluxo entre os serviços e medalhões.
- `docs/silver-rules.md`: responsabilidades planejadas para cada transformação Silver.
- `archive/legacy_notebooks/`: notebooks anteriores preservados como referência.
- `archive/legacy_dashboards/`: dashboards anteriores preservados como referência.

## Arquivos auxiliares

- `entrypoint.sh`: executa um comando recebido e, quando configurado, carrega variáveis pelo Infisical.
- `Dockerfile`: modelo ainda não configurado para gerar uma imagem executável.
- `.env.example`: arquivo atualmente vazio, reservado para documentar futuras variáveis de ambiente.

## Estado atual

A estrutura das camadas Silver e Gold está preparada, mas os novos arquivos SQL e notebooks ainda estão vazios. Nenhuma transformação, exportação ou pipeline é executada automaticamente pelo conteúdo atual do repositório.
