# Transformações Silver

As regras de transformação estão em
`pipelines/00_medallion/transformations/note_silver.py`.
O notebook único grava as mesmas 24 tabelas Delta gerenciadas em
`analytics.silver` a partir de `analytics.bronze_core`:

- Empresas, fornecedores, demandantes, técnicos e vínculos.
- Marcas, modelos, ofertas e estoque.
- Propostas, itens e unidades de proposta.
- Unidades locais, projetos, atividades, serviços, contratos, executores e avaliações.
- Planos, assinaturas, cobranças e contas de energia.

O módulo seleciona e renomeia campos, normaliza textos e status e preserva IDs de relacionamento. Não há deduplicação, histórico de alterações nem validação completa de qualidade implementados nesta versão. Os detalhes de cada coluna estão no dicionário `SPECS` do módulo.

A Silver alimenta as tabelas Gold. Os dashboards que consumirão a Gold ainda serão implementados em outra etapa; nenhum dashboard novo faz parte desta entrega.
