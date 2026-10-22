# Arquitetura de medalhões

O `databricks-sync` é responsável pela ingestão CoreDB → Bronze. Neste primeiro ciclo, a Bronze foi preenchida manualmente com um snapshot QA para validar as transformações; a ingestão contínua ainda não foi comprovada.

No `databricks-stocker`, a pipeline Silver lê `workspace.bronze_core` e publica 24 views em `workspace.silver_core`. A pipeline Gold lê a Silver e define 19 views em `workspace.gold_core`: as 11 anteriores e 8 KPIs de fornecedor. Um job versionado em `exports/medallion_export_job.json` encadeia Silver, Gold e chamada ao endpoint `databricks-sync /sync/gold` após sucesso. O job não foi instalado nem executado nesta alteração.

A Gold fornece dados para a próxima etapa, mas nenhum dashboard da nova arquitetura foi implementado nesta branch. Os JSONs em `archive/legacy_dashboards/` são referência da arquitetura anterior. O filtro por fornecedor exige `security/supplier_access.sql` e vínculo explícito entre identidade e fornecedor.

O exportador do `databricks-sync` copia as 19 views autorizadas para tabelas homônimas em `analyticsdb.analytics_gold`, com Row Level Security no PostgreSQL. É necessário provisionar o schema, o job, os segredos, os privilégios e os vínculos de identidade antes do uso real.
