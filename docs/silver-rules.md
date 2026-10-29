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

O módulo seleciona e renomeia campos, normaliza textos e status, converte medidas, indicadores e datas e preserva IDs de relacionamento. Antes de gravar qualquer tabela Silver, verifica as colunas exigidas, IDs vazios ou duplicados e valores que não podem ser convertidos. Os campos numéricos aceitam células numéricas ou texto decimal, inclusive notação científica quando a conversão é exata. Valores que perderiam casas decimais ou excederiam o tipo de destino são rejeitados. Datas não podem conter horário diferente de meia-noite; timestamps preservam o horário. Booleanos aceitam `t`/`true`/`1` e `f`/`false`/`0`, sem distinção de maiúsculas. Registros inválidos são retirados da Silver e a execução informa a fonte e a contagem por regra; a Bronze é preservada. Para IDs duplicados, todas as linhas com o mesmo ID são retiradas, sem escolher uma versão arbitrariamente. Colunas exigidas ausentes ainda interrompem a execução. Valores vazios em campos opcionais tornam-se `NULL`; colunas extras da Bronze são ignoradas. Os detalhes de cada coluna estão no dicionário `SPECS` do módulo. A Silver valida status conhecidos do schema versionado, estoque e disponibilidade não negativos e quantidades de itens de proposta positivas. Estoque e disponibilidade zero são válidos. Linhas com quantidade ou disponibilidade obrigatória vazia são omitidas da Silver e contabilizadas no log; a Bronze permanece intacta. Status desconhecidos e medidas fora dessas faixas são retirados da Silver e informados na saída da execução. Os status comerciais de oferta ainda não têm enum confirmado no schema consultado. Em serviços técnicos, a data de aceite e a data de conclusão não podem anteceder a criação; quando há aceite, a conclusão também não pode antecedê-lo. Datas vazias não são rejeitadas por essa comparação. Linhas com essas inconsistências são retiradas da Silver e informadas na saída da execução. Integridade entre tabelas e histórico de alterações ainda não são validados.

A Silver alimenta as tabelas Gold. Os dashboards que consumirão a Gold ainda serão implementados em outra etapa; nenhum dashboard novo faz parte desta entrega.
