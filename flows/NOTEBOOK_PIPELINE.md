<p align="center">
  <img src="../assets/readme/banners/databricks.png" alt="Databricks" width="100%">
</p>

# Transformação no workspace

O job executa uma transformação manual e chama a exportação somente depois do sucesso da primeira task.

```mermaid
flowchart TD
  A[Validar catálogo e modo manual] --> B[Conferir objetos existentes]
  B -->|View ou formato incompatível sem override| X[Parar para cutover]
  B -->|Destino permitido| C[Ler duas planilhas]
  C --> D[Gravar 25 tabelas Bronze]
  D --> E[Validar fontes Silver]
  E --> F[Gravar 24 tabelas Silver]
  F --> G[Calcular e gravar 19 tabelas Gold]
  G --> H[Task posterior de exportação]
  H -->|HTTP com Bearer| I[Serviço de sincronização]
  I -->|Contrato divergente| J[Falhar exportação sem afirmar publicação]
  I -->|Contrato alinhado e sucesso| K[Resumo da exportação]
```

## Falhas e limites

A ausência de uma aba ou coluna impede a leitura correspondente, e uma falha pode deixar atualizadas as tabelas processadas antes dela. O job declara uma única execução concorrente, e a exportação só funciona depois que o exportador adotar o contrato deste repositório.

Fonte em [implementação](https://github.com/Solierrr/databricks-stocker/blob/main/pipelines/00_medallion/transformations/my_transformation.py). O contexto integrado está em [cópia analítica](https://github.com/Solierrr/docs-warehouse/blob/main/docs/flows/ANALYTICS_SYNC.md).
