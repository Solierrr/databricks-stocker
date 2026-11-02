<p align="center">
  <img src="../assets/readme/banners/databricks.png" alt="Databricks" width="100%">
</p>

# Cálculo local dos indicadores

O executor local reutiliza as transformações e gera JSON sem tocar no workspace ou chamar o exportador.

```mermaid
flowchart TD
  A[Validar arquivos e Java] --> B[Ler Excel com openpyxl]
  B --> C[Representar células como texto]
  C --> D[Executar funções Bronze e Silver]
  D --> E[Views temporárias Spark]
  E --> F[Calcular Gold]
  F -->|Falha| G[Preservar arquivo anterior]
  F -->|Todos concluídos| H[Serializar decimais como texto]
  H --> I[Gravar arquivo temporário]
  I --> J[Substituir JSON final]
```

## Falhas e limites

A execução local não aplica row filter, não escreve em Delta nem exporta por HTTP, e as fórmulas não são recalculadas. Um resultado anterior pode continuar no lugar depois de uma falha, por isso convém conferir a geração e a conclusão, lembrando que o conjunto local pode conter vários fornecedores.

Fonte em [implementação](https://github.com/Solierrr/databricks-stocker/blob/main/run_local.py). O contexto integrado está em [cópia analítica](https://github.com/Solierrr/docs-warehouse/blob/main/docs/flows/ANALYTICS_SYNC.md).
