# databricks-stocker

Repositório de versionamento do que é construído no Databricks da Solaria: dashboards, pipelines e notebooks. Funciona como uma Git folder do workspace, para que o trabalho feito no Databricks passe por revisão e histórico como qualquer outro código da organização.

## Estrutura

- `dashboards/`: dashboards do Lakeview exportados como `.lvdash.json` (desempenho de vendas e indicadores administrativos).
- `pipelines/`: notebooks dos pipelines de BI que transformam as camadas do lake e alimentam os indicadores (catálogo, consumo energético, conversão de propostas, financeiro, serviço técnico e usuários).
- `docs/`: documentação do repositório.

## Relação com o databricks-sync

O `databricks-sync` leva os dados dos bancos (`coredb`, no Postgres) para a camada bronze do Databricks e devolve os resultados da camada gold para o banco de analytics, de onde o front carrega os KPIs. Este repositório guarda o que roda dentro do Databricks; o `databricks-sync` guarda a API que move os dados entre os bancos e o lake.

Este repositório não é um serviço: não tem imagem, API nem deploy automático.
