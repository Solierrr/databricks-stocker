<p align="center">
  <img src="../assets/readme/banners/databricks.png" alt="Databricks" width="100%">
</p>

# Situação

Levantamento da main `4b1ce020`, em 2026-10-31.

- O fluxo atual lê duas planilhas e escreve 25 tabelas Bronze, 24 Silver e 19 Gold. As antigas pastas de pipelines SQL não fazem parte desta árvore (2026-10-31).
- A exportação usa um endpoint existente no serviço de sincronização, e o contrato deste repositório é a referência que o destino deve seguir, adaptação ainda pendente no exportador (2026-10-31).
- O repositório contém duas definições de painéis, apesar de o texto de origem dizer que elas ainda não existem (2026-10-31).
- A qualidade exclui os registros inválidos e todos os registros com id duplicado, e as contagens são impressas, sem tabela persistente de rejeições (2026-10-31).
- As dez tabelas de fornecedor recebem filtro por principal, e as outras nove tabelas Gold dependem de grants por persona (2026-10-31).
- Execução local reutiliza transformações, sem aplicar row filter, escrever Delta ou acionar exportação. Seu JSON reúne os fornecedores presentes nas entradas (2026-10-31).
- Substituições são por tabela, sem uma transação global das três camadas. Falha pode deixar parte do conjunto atualizada (2026-10-31).
- Contagens atuais não são séries históricas. Aceite por mês usa data de criação como aproximação e não data efetiva de aceite (2026-10-31).
- Não há tabelas de pedidos, itens de pedido ou pagamentos na Silver atual. Os indicadores não devem ser descritos como receita realizada de pedidos (2026-10-31).
