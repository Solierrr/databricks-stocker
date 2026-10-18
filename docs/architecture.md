# Arquitetura de medalhões

O `databricks-sync` é responsável pela ingestão CoreDB → Bronze. Neste primeiro ciclo, a Bronze foi preenchida manualmente com um snapshot QA para validar as transformações; a ingestão contínua ainda não foi comprovada.

No `databricks-stocker`, a pipeline Silver lê `workspace.bronze_core` e publica 24 views em `workspace.silver_core`. A pipeline Gold lê a Silver e publica 11 views em `workspace.gold_core`. Ambas foram configuradas na interface do Databricks em modo Triggered e tiveram uma execução manual concluída com o snapshot QA.

Dashboards por persona e exportação Gold → AnalyticsDB → API/front são etapas futuras. Antes de publicar dados por empresa, será necessário definir e aplicar o isolamento de acesso na camada consumida.
