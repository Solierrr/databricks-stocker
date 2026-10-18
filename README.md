# Databricks Stocker

Workspace analítico da Solaria para transformar dados da camada Bronze em Silver e Gold no Databricks. A ingestão do CoreDB para Bronze pertence ao serviço `databricks-sync` e não faz parte deste repositório.

## Fluxo

```text
CoreDB -> Bronze -> Pipeline Silver -> Silver -> Pipeline Gold -> Gold
                                                        |-> dashboards (futuro)
                                                        `-> AnalyticsDB (exportação futura)
```

- **Bronze (`workspace.bronze_core`):** dados de origem. A carga QA usada nesta primeira validação foi importada manualmente de arquivos CSV; isso não configura a sincronização contínua do `databricks-sync`.
- **Silver (`workspace.silver_core`):** 24 views materializadas que selecionam campos, normalizam textos e status e preservam as chaves para relacionamentos. As regras ainda podem evoluir com o domínio.
- **Gold (`workspace.gold_core`):** 11 views materializadas com contagens, estoque, serviços e avaliações derivados da Silver. Cada indicador informa seu recorte atual; os KPIs de negócio ainda são uma primeira versão.

As pipelines Silver e Gold foram criadas na interface do Databricks em modo **Triggered**, com `transformations/` como código-fonte. Uma execução manual de cada pipeline foi concluída com a carga QA. O repositório contém as transformações SQL, mas não cria pipelines, agenda jobs nem executa cargas por si só.

## Estrutura

```text
pipelines/
  01_silver/
    transformations/       # SQL da Bronze para a Silver
    explorations/          # notebooks auxiliares, ainda sem lógica de validação
  02_gold/
    transformations/       # SQL da Silver para a Gold
    explorations/          # notebook auxiliar, ainda sem lógica de validação
exports/                   # notebook reservado para exportar Gold ao AnalyticsDB
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
- `02_suppliers.sql` (2 views): ofertas por status e unidades em estoque por modelo e fornecedor.
- `03_requesters.sql` (3 views): resumo de unidades, propostas e projetos por demandante; propostas e projetos por status.
- `04_professionals.sql` (3 views): serviços atribuídos por vínculo, serviços por status e avaliações ativas por técnico.

Os vínculos profissionais vêm da origem como `INDEPENDENT`, `AFFILIATED` ou `PARTNER`. A correspondência deles com as personas do aplicativo ainda precisa ser definida pelo time. Contagens e estados atuais não representam séries históricas.

## Dashboards: próxima etapa

Nenhum dashboard da nova arquitetura foi implementado nesta branch. As 11 views Gold estão disponíveis como fonte inicial; outro integrante do time ainda criará os painéis no Databricks para administração da Solaria, empresa fornecedora, empresa demandante, profissional contratado e profissional autônomo. O desenho dos painéis, os filtros e a correspondência dos vínculos profissionais com essas personas ainda precisam ser definidos.

A pasta `dashboards/` está reservada para os arquivos exportados desses painéis. Os dashboards de `archive/legacy_dashboards/` pertencem à arquitetura anterior e não devem ser tratados como entrega atual. Antes da publicação, o acesso aos dados de cada empresa precisará ser restringido na camada consumida; filtros visuais não substituem essa autorização.

## Outras etapas futuras

O notebook `exports/export_gold_to_analyticsdb.ipynb` é apenas um espaço reservado. A exportação da Gold para o AnalyticsDB e o consumo pela API/front ainda não foram implementados. Os notebooks de `archive/legacy_notebooks/` também são referências anteriores e não participam das pipelines atuais.

`entrypoint.sh`, `Dockerfile` e `.env.example` são auxiliares existentes, sem implantação do fluxo analítico por este repositório.
