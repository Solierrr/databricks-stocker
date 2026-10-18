# Arquitetura de medalhões

O `databricks-sync` é responsável pela ingestão CoreDB → Bronze. Neste primeiro ciclo, a Bronze foi preenchida manualmente com um snapshot QA para validar as transformações; a ingestão contínua ainda não foi comprovada.

No `databricks-stocker`, a pipeline Silver lê `workspace.bronze_core` e publica 24 views em `workspace.silver_core`. A pipeline Gold lê a Silver e publica 11 views em `workspace.gold_core`. Ambas foram configuradas na interface do Databricks em modo Triggered e tiveram uma execução manual concluída com o snapshot QA.

A Gold já fornece dados para a próxima etapa, mas nenhum dashboard da nova arquitetura foi implementado nesta branch. Outro integrante criará os painéis por persona no Databricks; os JSONs em `archive/legacy_dashboards/` são apenas referência da arquitetura anterior.

A exportação Gold → AnalyticsDB → API/front também é futura. Antes de publicar dashboards ou dados por empresa, será necessário definir e aplicar o isolamento de acesso na camada consumida.
