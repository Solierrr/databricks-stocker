# Databricks Stocker

Workspace analítico da Solaria para transformar Bronze em Silver e Gold no Databricks. O novo modo de ingestão manual usa dois Excel e um único notebook de transformação em [`pipelines/00_medallion/`](pipelines/00_medallion/README.md). A ingestão CoreDB → Bronze do `databricks-sync` continua disponível como fluxo anterior; não foi removida.

## Fluxo

```text
fornecedores.xlsx + admins.xlsx -> notebook único de transformação:
                                 Bronze -> Silver -> Gold
                                                   |-> dashboards (futuro)
                                                   `-> databricks-sync /sync/gold -> AnalyticsDB
```

- **Bronze (`analytics.bronze_core`):** 25 entidades lidas de abas dos dois Excel no novo modo manual. A carga QA anterior veio de CSV; o serviço CoreDB → Bronze permanece como caminho anterior e exige cutover antes de publicar nos mesmos nomes.
- **Silver (`analytics.silver`):** 24 tabelas Delta gerenciadas que selecionam campos, normalizam textos e status e preservam as chaves para relacionamentos. As regras ainda podem evoluir com o domínio.
- **Gold (`analytics.gold`):** 19 tabelas Delta gerenciadas: os 11 indicadores existentes e 8 métricas adicionais do dashboard do fornecedor, calculadas a partir da Silver.

As pipelines Silver e Gold anteriores foram criadas na interface do Databricks em modo **Triggered**. A definição atual neste repositório é um job com uma task de notebook Bronze → Silver → Gold e uma task posterior de exportação. Antes de executar, revise o serverless da conta e faça o cutover das views controladas pelas pipelines antigas no workspace. Remover os arquivos antigos do repositório não exclui essas pipelines do Databricks. Nada foi instalado ou executado nesta migração.

## Estrutura

```text
pipelines/
  00_medallion/
    transformations/       # notebook orquestrador e módulos Bronze, Silver e Gold
exports/                   # job e notebook que acionam databricks-sync após Gold
security/                  # row filter e mapa de acesso do fornecedor
```

### Transformações Silver

- Empresas, fornecedores, demandantes, técnicos e vínculos.
- Marcas, modelos, ofertas e estoque.
- Propostas, itens e unidades de proposta.
- Unidades locais, projetos, atividades, serviços, executores e avaliações.
- Planos, assinaturas, cobranças e contas de energia.

### Indicadores Gold (`pipelines/00_medallion/transformations/note_gold.py`)

- Administração (3 tabelas): fornecedores ativos, demandantes cadastrados, modelos e propostas por status.
- Fornecedores (10 tabelas): ofertas, estoque, propostas e valor aceito, com row filter.
- Demandantes (3 tabelas): resumo de unidades, propostas e projetos; propostas e projetos por status.
- Profissionais (3 tabelas): serviços atribuídos por vínculo, serviços por status e avaliações ativas por técnico.

Os vínculos profissionais vêm da origem como `INDEPENDENT`, `AFFILIATED` ou `PARTNER`. A correspondência deles com as personas do aplicativo ainda precisa ser definida pelo time. Contagens e estados atuais não representam séries históricas.

## Dashboards: próxima etapa

Nenhum dashboard da nova arquitetura foi implementado nesta branch. As 19 tabelas Gold são fontes para os painéis. O desenho dos painéis e a correspondência dos vínculos profissionais com as personas ainda precisam ser definidos.

As tabelas de fornecedor usam row filter; provisione `security/supplier_access.sql` e cadastre a identidade do job e do exportador como ADMIN antes da primeira atualização. Não conceda SELECT amplo nas demais tabelas a usuários finais antes de definir políticas por persona.

## Outras etapas futuras

A exportação usa `exports/trigger_gold_sync.py` para chamar a API após o notebook de transformação. O job, o modo de execução serverless, as permissões e os segredos ainda precisam ser provisionados no Databricks; nenhum serviço foi acionado nesta alteração.

## Execução local dos KPIs

Use `python -B run_local.py` ou abra `rodar_local.bat` no Windows para executar Bronze → Silver → Gold com os Excel locais e gerar `local-output/gold_kpis.json`. Consulte a [preparação e os limites da execução local](docs/execucao-local.md).
