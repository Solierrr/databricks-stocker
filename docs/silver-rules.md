# Transformações Silver

As regras de transformação estão em
`pipelines/00_medallion/transformations/note_silver.py`.
O notebook único grava as mesmas 24 tabelas Delta gerenciadas em
`workspace.silver` a partir de `workspace.bronze_core`:

- Empresas, fornecedores, demandantes, técnicos e vínculos.
- Marcas, modelos, ofertas e estoque.
- Propostas, itens e unidades de proposta.
- Unidades locais, projetos, atividades, serviços, contratos, executores e avaliações.
- Planos, assinaturas, cobranças e contas de energia.

O módulo seleciona e renomeia campos, normaliza textos e status, converte medidas, indicadores e datas e preserva IDs de relacionamento. Antes de gravar cada tabela Silver, verifica as colunas exigidas, IDs vazios ou duplicados e valores que não podem ser convertidos. Nessas situações, a execução para com uma mensagem que identifica a fonte e os campos inválidos. Valores vazios em campos opcionais tornam-se `NULL`; colunas extras da Bronze são ignoradas. Os detalhes de cada coluna estão no dicionário `SPECS` do módulo. Regras de domínio específicas, integridade entre tabelas e histórico de alterações não estão definidos neste contrato.

A Silver alimenta as tabelas Gold. Os dashboards que consumirão a Gold ainda serão implementados em outra etapa; nenhum dashboard novo faz parte desta entrega.
