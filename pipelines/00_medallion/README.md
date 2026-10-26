# Fluxo único em notebook: Excel → Bronze → Silver → Gold

`transformations/my_transformation.py` é o notebook de transformação. Ele chama
`note_bronze.py`, `note_silver.py` e `note_gold.py` na ordem e grava **tabelas
Delta gerenciadas comuns**, com os mesmos nomes das antigas views: 25 Bronze,
24 Silver e 19 Gold. Os KPIs continuam em PySpark; somente a criação das dez
tabelas Gold de fornecedor usa DDL SQL para aplicar o row filter. Não há três
pipelines, nem uma Lakeflow Declarative Pipeline ativa nesta pasta.

O job `exports/medallion_export_job.json` tem a task desse notebook e uma task
dependente que chama `databricks-sync /sync/gold` para exportar ao AnalyticsDB.
A exportação só começa se a transformação terminar com sucesso. O job não tem
agendamento nem compute explicitamente configurado: revise o modo serverless
disponível na conta antes de implantá-lo.
Nada foi instalado ou executado por esta alteração.
O parâmetro `source_mode` aceita somente `manual` nesta versão; CoreDB → Bronze
automático permanece no serviço `databricks-sync` e não é acionado por este job.

## Contrato das planilhas

Carregue dois arquivos `.xlsx` em um Unity Catalog Volume. Os caminhos no
job são exemplos configuráveis. Cada entidade abaixo é uma aba com esse nome,
cabeçalho na primeira linha e uma linha por registro. `SHEETS` em
`note_bronze.py` enumera as colunas mínimas; colunas adicionais permanecem na
Bronze. Falta de aba ou coluna causa falha explícita.

| Arquivo | Abas |
|---|---|
| `fornecedores.xlsx` | `brand`, `model`, `supplier`, `offer`, `inventory`, `proposal`, `proposal_item` |
| `admins.xlsx` | `company`, `person`, `requester`, `technician`, `technician_affiliation`, `user_company`, `proposal_unit`, `local_unit`, `professional_review`, `project_activity`, `service_contract`, `service_executor`, `technical_project`, `technical_service`, `charge`, `company_plan`, `subscription`, `energy_bill` |

As 18 abas de admins incluem as fontes de demandante e profissional para
preservar os 19 KPIs. As 7 abas de fornecedores incluem relações compartilhadas
com KPIs administrativos. IDs e FKs devem ser coerentes entre abas. Formate
UUIDs como texto; datas, números e booleanos precisam ter tipos adequados no
Excel. A leitura nativa exige Databricks Runtime 17.1 ou superior.

Nomes de abas e colunas seguem o baseline renomeado do CoreDB (views do schema
`export`): FKs como `<tabela>_id`, booleanos com `is_`/`has_`, datas com
`_at`, `company_plan` no singular e `model_name`. Os arquivos em `recovery/`
ainda usam os nomes anteriores e precisam ser regerados a partir das views
`export` antes de uma execução. O catálogo é o parâmetro `catalog` do job
(`analytics`; `analyticsqa` em QA), e os volumes dos caminhos devem pertencer
a esse catálogo. Os quatro indicadores condicionais de pedidos do documento
original não foram adicionados: não há essas entidades na Silver.

## Implantação e cutover

Se `my_transformation` falhar com `read_bronze() takes 3 positional arguments
but 4 were given`, atualize **os dois** arquivos
`transformations/my_transformation.py` e `transformations/note_bronze.py` no
workspace. A versão atual passa o caminho do Excel como quarto argumento e
`read_bronze` aceita esse argumento; chamadas antigas com três argumentos
continuam válidas. Reinicie a sessão Python do notebook antes de executar de
novo, pois módulos já importados podem permanecer em cache.

1. Revise as duas planilhas e os caminhos, e disponibilize os quatro arquivos
   Python no mesmo diretório do workspace. Revise o runtime, o caminho do
   notebook no job e a configuração de execução serverless da conta.
2. Provisione `analytics.gold_security.can_read_supplier` e o mapa de acesso
   conforme `security/supplier_access.sql`. Conceda acesso ADMIN à identidade
   que executará o notebook e à identidade exportadora.
3. Preserve snapshots das tabelas Bronze e das views Silver/Gold atuais.
   Desative as pipelines antigas e resolva a propriedade e os nomes de seus
   objetos antes da primeira execução. Não há conversão automática de uma view
   materializada controlada por pipeline para uma tabela Delta comum.
4. O parâmetro `allow_replace_existing` começa em `false`; o notebook aborta
   se algum dos 68 nomes já existir. Só use `true` após concluir o cutover e
   revisar quais tabelas Delta comuns serão substituídas.
5. Instale o job e configure os segredos da task de exportação. Cada execução
   regrava as tabelas na ordem Bronze → Silver → Gold e então chama a API Gold.

Uma execução lê dois Excel, grava 68 tabelas e usa compute Databricks. Um job
único não implica consumo zero da cota. Na Free Edition, o compute é
serverless e sujeito a limite de uso; revise a cota antes de executar.
As definições SQL anteriores não são fontes ativas deste job.
