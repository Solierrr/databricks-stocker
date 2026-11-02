<p align="center">
  <img src="../assets/readme/banners/databricks.png" alt="Databricks" width="100%">
</p>

# Visão de produto

O processamento reúne dados de negócio em indicadores para acompanhar cadastro, estoque, negociação e execução. Ele atende à operação analítica e às equipes que publicam consultas e painéis, sem ser uma API de negócio para os aplicativos.

## Recortes disponíveis

- Administração acompanha fornecedores ativos, demandantes, modelos e propostas por situação.
- Fornecedores acompanham ofertas, preço anunciado, vencimentos, disponibilidade, estoque e propostas recebidas.
- Demandantes acompanham unidades, propostas e projetos, inclusive contagens por situação.
- Profissionais acompanham serviços atribuídos por vínculo, situação dos serviços e avaliações ativas.

Os indicadores são recalculados sobre o conjunto carregado. Eles não mantêm uma série histórica de cada mudança. Valor aceito de proposta é preço negociado vezes quantidade, sem aplicar o desconto uma segunda vez. Quando o preço está ausente, não se inventa um valor. A cobertura de preço é exposta em indicador próprio.

A data de criação da proposta aproxima a distribuição temporal dos aceites. Não representa a data comprovada de aceite. A taxa de aceite usa propostas aceitas entre as encerradas, incluindo recusadas e canceladas no denominador.

## Qualidade

Textos são normalizados, medidas e datas são convertidas e linhas inválidas são retiradas da camada tratada. Todos os registros com identificador duplicado são retirados, sem escolher uma versão ao acaso. Estoque e disponibilidade podem ser zero. Quantidades de itens e unidades de proposta devem ser positivas.

O processo informa as contagens de rejeição no terminal, sem criar um cadastro persistente de qualidade, e os recortes por tipo de vínculo profissional seguem os filtros implementados.

## Consulta e acesso

Há arquivos de dois painéis exportados, um para a administração e outro para o fornecedor, e a documentação antiga que dizia que a pasta estava reservada já não descreve o conteúdo atual.

O acesso de fornecedor tem filtro por identidade no ambiente Databricks, e a execução local não aplica esse filtro, incluindo todos os fornecedores presentes nas planilhas.
