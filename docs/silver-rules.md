# Transformações Silver

As seis transformações SQL de `pipelines/01_silver/transformations/` criam 24 views materializadas em `workspace.silver_core` a partir de `workspace.bronze_core`:

- `01_companies.sql`: empresas, fornecedores, demandantes, técnicos e vínculos.
- `02_marketplace.sql`: marcas, modelos, ofertas e estoque.
- `03_proposals.sql`: propostas, itens e unidades de proposta.
- `04_services.sql`: unidades locais, projetos, atividades, serviços, contratos, executores e avaliações.
- `05_finance.sql`: planos, assinaturas e cobranças.
- `06_energy.sql`: contas de energia.

Os arquivos selecionam e renomeiam campos, normalizam textos e status e preservam IDs de relacionamento. Não há deduplicação, histórico de alterações nem validação completa de qualidade implementados nesta versão. Os detalhes de cada coluna estão nos próprios SQLs.

A Silver alimenta as views Gold. Os dashboards que consumirão a Gold ainda serão implementados em outra etapa; nenhum dashboard novo faz parte desta entrega.
