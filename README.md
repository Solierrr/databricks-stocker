# databricks-stocker

Notebook de migração manual sob demanda, Postgres (`core` + `auth`) →
Databricks (`workspace.core` / `workspace.auth`). Alternativa ao
`databricks-sync` (que sincroniza automaticamente a cada 4h via GitHub
Actions): este roda dentro do próprio workspace Databricks, disparado
manualmente, e não depende de nenhuma credencial ou serviço externo — lê as
credenciais do Postgres direto de um secret scope nativo do Databricks.

## Como rodar

1. No workspace Databricks, crie o secret scope (uma vez, via
   [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/)):

   ```
   databricks secrets create-scope solaria-db
   databricks secrets put-secret solaria-db core-host
   databricks secrets put-secret solaria-db core-port
   databricks secrets put-secret solaria-db core-name
   databricks secrets put-secret solaria-db core-user
   databricks secrets put-secret solaria-db core-password
   databricks secrets put-secret solaria-db auth-host
   databricks secrets put-secret solaria-db auth-port
   databricks secrets put-secret solaria-db auth-name
   databricks secrets put-secret solaria-db auth-user
   databricks secrets put-secret solaria-db auth-password
   ```

   Cada `put-secret` abre um prompt para colar o valor (não fica no
   histórico do shell).

2. Sincronize este repositório com o workspace via **Git folder**
   (Databricks Repos) — `src/migrate.py` já tem o cabeçalho de notebook
   nativo (`# Databricks notebook source`) e abre diretamente como notebook.

3. Rode o notebook manualmente (ou agende como Job, se quiser recorrência).
   O progresso é impresso tabela a tabela; ao final imprime
   `Migracao concluida.`.

Se as credenciais do Postgres rotacionarem, atualize só o secret
correspondente (`databricks secrets put-secret solaria-db core-password`) —
não precisa reimplantar nada.
