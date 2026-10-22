# Databricks Stocker

Workspace analítico da Solaria para transformar dados da camada Bronze em Silver e Gold no Databricks. A ingestão do CoreDB para Bronze pertence ao serviço `databricks-sync` e não faz parte deste repositório.

## Fluxo

```text
CoreDB -> Bronze -> Pipeline Silver -> Silver -> Pipeline Gold -> Gold
                                                        |-> dashboards (futuro)
                                                        `-> databricks-sync /sync/gold -> AnalyticsDB
```

- **Bronze (`workspace.bronze_core`):** dados de origem. A carga QA usada nesta primeira validação foi importada manualmente de arquivos CSV; isso não configura a sincronização contínua do `databricks-sync`.
- **Silver (`workspace.silver_core`):** 24 views materializadas que selecionam campos, normalizam textos e status e preservam as chaves para relacionamentos. As regras ainda podem evoluir com o domínio.
- **Gold (`workspace.gold_core`):** 19 views materializadas: as 11 existentes e 8 métricas adicionais do dashboard do fornecedor, calculadas a partir da Silver.

As pipelines Silver e Gold foram criadas na interface do Databricks em modo **Triggered**, com `transformations/` como código-fonte. Uma execução manual anterior de cada pipeline foi concluída com a carga QA. O novo `exports/medallion_export_job.json` descreve a ordem Silver → Gold → exportação; ele ainda precisa ser instalado e seus IDs e caminho confirmados no workspace.

## Estrutura

```text
pipelines/
  01_silver/
    transformations/       # SQL da Bronze para a Silver
    explorations/          # notebooks auxiliares, ainda sem lógica de validação
  02_gold/
    transformations/       # SQL da Silver para a Gold
    explorations/          # notebook auxiliar, ainda sem lógica de validação
exports/                   # job e notebook que acionam databricks-sync após Gold
security/                  # row filter e mapa de acesso do fornecedor
dashboards/                # reservado para os dashboards futuros
archive/                   # notebooks e dashboards da arquitetura anterior
```

### Transformações Silver

- `01_companies.sql`: empresas, fornecedores, demandantes, técnicos e vínculos.
- `02_marketplace.sql`: marcas, modelos, ofertas e estoque.
- `03_proposals.sql`: propostas, itens e unidades de proposta.
- `04_services.sql`: unidades locais, projetos, atividades, serviços, executores e avaliações.
- `05_finance.sql`: planos, assinaturas e cobranças.
- `06_energy.sql`: contas de energia.

### Indicadores Gold

- `01_admin.sql` (3 views): fornecedores ativos, demandantes cadastrados, modelos e propostas por status.
- `02_suppliers.sql` (2 views): ofertas por status e unidades em estoque por modelo e fornecedor, com row filter.
- `05_supplier_kpis.sql` (8 views): resumo/detalhe de ofertas, estoque, propostas e valor aceito por produto e mês.
- `03_requesters.sql` (3 views): resumo de unidades, propostas e projetos por demandante; propostas e projetos por status.
- `04_professionals.sql` (3 views): serviços atribuídos por vínculo, serviços por status e avaliações ativas por técnico.

Os vínculos profissionais vêm da origem como `INDEPENDENT`, `AFFILIATED` ou `PARTNER`. A correspondência deles com as personas do aplicativo ainda precisa ser definida pelo time. Contagens e estados atuais não representam séries históricas.

## Dashboards: próxima etapa

Nenhum dashboard da nova arquitetura foi implementado nesta branch. As 19 views Gold são fontes para os painéis; o dashboard do fornecedor pode usar a consulta em `pipelines/02_gold/queries/fornecedor_produtos_sem_aceite_periodo.sql` para um intervalo selecionado. O desenho dos demais painéis e a correspondência dos vínculos profissionais com as personas ainda precisam ser definidos.

A pasta `dashboards/` está reservada para os arquivos exportados desses painéis. Os dashboards de `archive/legacy_dashboards/` pertencem à arquitetura anterior. As views de fornecedor usam row filter; execute `security/supplier_access.sql` e cadastre a identidade do pipeline e do exportador como ADMIN antes da primeira atualização. Não conceda SELECT amplo nas demais views a usuários finais antes de definir políticas por persona.

## Outras etapas futuras

O notebook antigo `exports/export_gold_to_analyticsdb.ipynb` segue como espaço reservado. A exportação usa `exports/trigger_gold_sync.py` para chamar a API após as duas pipelines. O job, as permissões e os segredos ainda precisam ser provisionados no Databricks; nenhum serviço foi acionado nesta alteração.

`entrypoint.sh`, `Dockerfile` e `.env.example` são auxiliares existentes, sem implantação do fluxo analítico por este repositório.
