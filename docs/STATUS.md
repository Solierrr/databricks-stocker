<p align="center">
  <img src="../assets/readme/banners/databricks.png" alt="Databricks" width="100%">
</p>

# Situação

Levantamento da main `4b1ce020`, em 2026-10-31.

- O fluxo atual lê duas planilhas e escreve 28 tabelas Bronze, 27 Silver e 27 Gold. As antigas pastas de pipelines SQL não fazem parte desta árvore (2026-10-31).
- A exportação usa um endpoint existente no serviço de sincronização, e o contrato deste repositório é a referência que o destino deve seguir, adaptação ainda pendente no exportador (2026-10-31).
- O repositório contém duas definições de painéis, apesar de o texto de origem dizer que elas ainda não existem (2026-10-31). Em 2026-11-02 cada uma ganhou páginas novas com filtros interativos, histogramas e boxplot de preço, e o painel de administração ganhou o mapa da demanda por estado. O formato dos widgets segue exportações reais de painéis do Databricks, e os testes de `tests/test_dashboards.py` conferem a estrutura. Falta abrir os dois arquivos num workspace para confirmar a renderização (2026-11-02). <!-- PENDENTE: abrir os painéis no Databricks -->
- A tabela `kpi_admin_demand_by_state` junta unidades cadastradas e propostas por estado, e usa a aba `address` das planilhas (só `id`, `state` e `city`). A coordenada de cada UF vem de `geography.py` e é aproximada (2026-11-02).
- A qualidade exclui os registros inválidos e todos os registros com id duplicado, e as contagens são impressas, sem tabela persistente de rejeições (2026-10-31).
- As quinze tabelas de fornecedor recebem filtro por principal, e as outras doze tabelas Gold dependem de grants por persona (2026-11-02).
- Execução local reutiliza transformações, sem aplicar row filter, escrever Delta ou acionar exportação. Seu JSON reúne os fornecedores presentes nas entradas (2026-10-31).
- Substituições são por tabela, sem uma transação global das três camadas. Falha pode deixar parte do conjunto atualizada (2026-10-31).
- Contagens atuais não são séries históricas. Aceite por mês usa data de criação como aproximação e não data efetiva de aceite (2026-10-31).
- Os pedidos e itens de pedido entram na Silver e na Gold (2026-11-02). Pagamento dos pedidos (`order_payment`) ainda não entra, e os indicadores de pedido refletem só os pedidos pagos e cancelados da planilha.
