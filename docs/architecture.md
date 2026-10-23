# Arquitetura de medalhões

O `databricks-sync` é responsável pela ingestão CoreDB → Bronze. Neste primeiro ciclo, a Bronze foi preenchida manualmente com um snapshot QA para validar as transformações; a ingestão contínua ainda não foi comprovada.

No `databricks-stocker`, o notebook `pipelines/00_medallion/transformations/my_transformation.py` lê 25 abas distribuídas em dois Excel e grava 25 tabelas Bronze, 24 Silver e 19 Gold como Delta gerenciadas comuns. O job versionado em `exports/medallion_export_job.json` executa essa transformação e chama `databricks-sync /sync/gold` somente após sucesso. A configuração ainda exige caminhos, runtime, compute e cutover dos objetos antigos; não foi instalada nem executada.

A Gold fornece dados para a próxima etapa, mas nenhum dashboard da nova arquitetura foi implementado nesta branch. Os JSONs em `archive/legacy_dashboards/` são referência da arquitetura anterior. O filtro por fornecedor exige `security/supplier_access.sql` e vínculo explícito entre identidade e fornecedor.

O exportador do `databricks-sync` copia as 19 tabelas Gold autorizadas para tabelas homônimas em `analyticsdb.analytics_gold`, com Row Level Security no PostgreSQL. É necessário provisionar o schema, o job, os segredos, os privilégios e os vínculos de identidade antes do uso real.
