<p align="center">
  <img src="../assets/readme/banners/databricks.png" alt="Databricks" width="100%">
</p>

# Execução local

Usar Python compatível com PySpark 4.2.0 e Java 17 ou 21 no PATH. `requirements-local.txt` fixa PySpark e declara openpyxl na faixa 3.1 a 4. O guia comum de Python está em [execução local](https://github.com/Solierrr/docs-warehouse/blob/main/readme/running/PYTHON.md), mas este repositório não possui Makefile, `make vault` ou API.

## Preparar

Executar no checkout do processamento.

```bash
python -m pip install -r requirements-local.txt
python -B run_local.py
```

O padrão lê `recovery/fornecedores.xlsx` e `recovery/admins.xlsx` e grava `local-output/gold_kpis.json`. As planilhas não foram abertas nem copiadas para esta documentação, e os exemplos devem ser validados com cópias sintéticas autorizadas.

```bash
python -B run_local.py --fornecedores fornecedores.xlsx --admins admins.xlsx --output local-output/gold_kpis.json
```

No Windows, `rodar_local.bat` mantém a janela aberta para mostrar o resultado. Se houver problema de loopback Java, `--temp-dir` aceita uma pasta curta e gravável própria para a execução.

## Saída

O JSON traz `metadata` e `tables`, com uma lista por indicador, as datas em ISO, os decimais como texto, os inteiros como números e as ausências como null. O fuso local é `America/Sao_Paulo`, e os indicadores que usam a data atual mudam conforme o dia da execução.

Conferir `metadata.generated_at`, modo local e a indicação de filtro de fornecedor não aplicado. Um arquivo anterior pode continuar existindo se a execução nova falhar. O executor usa valores armazenados das fórmulas Excel, sem recalcular as fórmulas.

## Verificações disponíveis

Os comandos são os scripts de teste existentes, sem alvos make.

```bash
python -m unittest discover -s tests -p 'test_*.py'
python tests/validate_local.py
python tests/validate_silver_spark.py
python tests/validate_temporal_spark.py
```

A primeira opção verifica contratos e executor com testes isolados. `validate_local.py` também inspeciona as planilhas de recuperação. Os dois scripts Spark exigem Java e PySpark e verificam conversões e regras temporais. Nenhum deles comprova permissões, escrita Delta ou execução no workspace.

A execução local coleta dados em memória. Não substitui o teste do leitor Excel nativo ou a revisão das permissões. Não há variável secreta exigida pelo executor local. No Databricks, as credenciais de exportação ficam no secret scope, descrito em [DEPLOY.md](./DEPLOY.md).

Fontes em [dependências](https://github.com/Solierrr/databricks-stocker/blob/main/requirements-local.txt), [executor](https://github.com/Solierrr/databricks-stocker/blob/main/run_local.py), [testes](https://github.com/Solierrr/databricks-stocker/tree/main/tests) e [instruções locais](https://github.com/Solierrr/databricks-stocker/blob/main/docs/execucao-local.md).
