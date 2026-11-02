<p align="center">
  <img src="./assets/readme/banners/databricks.png" alt="Databricks" width="100%">
</p>

<p align="center">
  <img src="./assets/readme/stacks/python.png" width="100" hspace="8" alt="Python" title="Python">
</p>

# Preparação dos indicadores de negócio

[![License](https://img.shields.io/github/license/Solierrr/databricks-stocker)](https://github.com/Solierrr/databricks-stocker/blob/main/LICENSE)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/Solierrr/databricks-stocker)](https://github.com/Solierrr/databricks-stocker/commits)
[![GitHub Pull Requests](https://img.shields.io/github/issues-pr/Solierrr/databricks-stocker)](https://github.com/Solierrr/databricks-stocker/pulls)
[![Release](https://img.shields.io/github/v/release/Solierrr/databricks-stocker)](https://github.com/Solierrr/databricks-stocker/releases)

Este repositório prepara indicadores de negócio para administração, fornecedores, demandantes e profissionais. Ele organiza dados de cadastro, ofertas, estoque, propostas e serviços em camadas de análise. A operação fornece duas planilhas, executa as transformações e acompanha as linhas aceitas ou retiradas pelas regras de qualidade. Os resultados representam o estado das entradas utilizadas e não uma atualização contínua do negócio.

Python e Spark transformam as planilhas em tabelas Delta no Databricks. Uma camada preserva a entrada, outra normaliza e valida os dados e a última calcula os indicadores. O mesmo cálculo pode rodar localmente e gerar JSON, com diferenças de armazenamento e acesso. Um job encadeia a transformação e a chamada ao serviço de exportação, cujo contrato ainda precisa ser alinhado.

## Aprofunde-se no Projeto!

- [OVERVIEW.md](./docs/OVERVIEW.md), indicadores e regras de produto.
- [ARCHITECTURE.md](./docs/ARCHITECTURE.md), camadas, módulos e integrações.
- [RUNNING.md](./docs/RUNNING.md), execução local e verificações.
- [DEPLOY.md](./docs/DEPLOY.md), instalação no workspace e exportação.
- [STATUS.md](./docs/STATUS.md), diferenças e limitações verificadas.
- [Fluxos](./flows/), transformação, execução local e acesso.
- [DATABRICKS.md](./database/DATABRICKS.md), inventário Bronze, Silver, Gold e segurança.
- [PENDENCIAS.md](./PENDENCIAS.md), confirmações do responsável.

## Contribuindo

- [CONTRIBUTING.md](https://github.com/Solierrr/.github/blob/main/.github/CONTRIBUTING.md), convenções de contribuição.
- [CODE_OF_CONDUCT.md](https://github.com/Solierrr/.github/blob/main/CODE_OF_CONDUCT.md), código de conduta.
- [SECURITY.md](https://github.com/Solierrr/.github/blob/main/SECURITY.md), reporte de vulnerabilidade.
