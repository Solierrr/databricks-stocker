# Databricks Stocker

Workspace analítico da Solaria para transformar Bronze em Silver e Gold no Databricks. O novo modo de ingestão manual usa dois Excel e um único notebook de transformação em [`pipelines/00_medallion/`](pipelines/00_medallion/README.md). A ingestão CoreDB → Bronze do `databricks-sync` continua disponível como fluxo anterior; não foi removida.

## Fluxo

```text
fornecedores.xlsx + admins.xlsx -> notebook único de transformação:
                                 Bronze -> Silver -> Gold
                                                   |-> dashboards (futuro)
                                                   `-> databricks-sync /sync/gold -> AnalyticsDB
```

- **Bronze (`workspace.bronze_core`):** 25 entidades lidas de abas dos dois Excel no novo modo manual. A carga QA anterior veio de CSV; o serviço CoreDB → Bronze permanece como caminho anterior e exige cutover antes de publicar nos mesmos nomes.
- **Silver (`workspace.silver_core`):** 24 tabelas Delta gerenciadas que selecionam campos, normalizam textos e status e preservam as chaves para relacionamentos. As regras ainda podem evoluir com o domínio.
- **Gold (`workspace.gold_core`):** 19 tabelas Delta gerenciadas: os 11 indicadores existentes e 8 métricas adicionais do dashboard do fornecedor, calculadas a partir da Silver.

As pipelines Silver e Gold anteriores foram criadas na interface do Databricks em modo **Triggered**. Seus arquivos estão em `archive/legacy_pipelines/`. A definição ativa neste repositório é um job com uma task de notebook Bronze → Silver → Gold e uma task posterior de exportação. Antes de executar, revise o serverless da conta e faça o cutover das views controladas pelas pipelines antigas. Nada foi instalado ou executado nesta migração.

## Estrutura

```text
pipelines/
  00_medallion/
    transformations/       # notebook orquestrador e módulos Bronze, Silver e Gold
exports/                   # job e notebook que acionam databricks-sync após Gold
security/                  # row filter e mapa de acesso do fornecedor
dashboards/                # reservado para os dashboards futuros
archive/
  legacy_pipelines/        # SQL, consultas e explorações das duas pipelines antigas
  recovery/                # configurações antigas, somente referência histórica
```

### Transformações Silver anteriores

- `01_companies.sql`: empresas, fornecedores, demandantes, técnicos e vínculos.
- `02_marketplace.sql`: marcas, modelos, ofertas e estoque.
- `03_proposals.sql`: propostas, itens e unidades de proposta.
- `04_services.sql`: unidades locais, projetos, atividades, serviços, executores e avaliações.
- `05_finance.sql`: planos, assinaturas e cobranças.
- `06_energy.sql`: contas de energia.

### Indicadores Gold anteriores (migrados para `00_medallion/transformations/note_gold.py`)

- `01_admin.sql` (3 tabelas): fornecedores ativos, demandantes cadastrados, modelos e propostas por status.
- `02_suppliers.sql` (2 tabelas): ofertas por status e unidades em estoque por modelo e fornecedor, com row filter.
- `05_supplier_kpis.sql` (8 tabelas): resumo/detalhe de ofertas, estoque, propostas e valor aceito por produto e mês.
- `03_requesters.sql` (3 tabelas): resumo de unidades, propostas e projetos por demandante; propostas e projetos por status.
- `04_professionals.sql` (3 tabelas): serviços atribuídos por vínculo, serviços por status e avaliações ativas por técnico.

Os vínculos profissionais vêm da origem como `INDEPENDENT`, `AFFILIATED` ou `PARTNER`. A correspondência deles com as personas do aplicativo ainda precisa ser definida pelo time. Contagens e estados atuais não representam séries históricas.

## Dashboards: próxima etapa

Nenhum dashboard da nova arquitetura foi implementado nesta branch. As 19 tabelas Gold são fontes para os painéis; o dashboard do fornecedor pode adaptar a consulta preservada em `archive/legacy_pipelines/02_gold/queries/fornecedor_produtos_sem_aceite_periodo.sql` para um intervalo selecionado. O desenho dos demais painéis e a correspondência dos vínculos profissionais com as personas ainda precisam ser definidos.

A pasta `dashboards/` está reservada para os arquivos exportados desses painéis. Os dashboards de `archive/legacy_dashboards/` pertencem à arquitetura anterior. As tabelas de fornecedor usam row filter; provisione `security/supplier_access.sql` e cadastre a identidade do job e do exportador como ADMIN antes da primeira atualização. Não conceda SELECT amplo nas demais tabelas a usuários finais antes de definir políticas por persona.

## Outras etapas futuras

O notebook antigo `exports/export_gold_to_analyticsdb.ipynb` segue como espaço reservado. A exportação usa `exports/trigger_gold_sync.py` para chamar a API após o notebook de transformação. O job, o modo de execução serverless, as permissões e os segredos ainda precisam ser provisionados no Databricks; nenhum serviço foi acionado nesta alteração.

`entrypoint.sh`, `Dockerfile` e `.env.example` são auxiliares existentes, sem implantação do fluxo analítico por este repositório.
