# Executar o medalhão localmente

O arquivo `run_local.py` executa o `run()` do `my_transformation.py`, com os módulos
Bronze, Silver, Gold e catálogo do projeto. Ele adapta a leitura de Excel para
`openpyxl` e substitui as gravações por views temporárias em uma sessão Spark local.
As regras de limpeza, validação e cálculo são as mesmas funções usadas no fluxo.

## Preparação

É necessário Python compatível com PySpark 4.2 e Java 17 ou 21 disponível no PATH.
Instale as dependências a partir da pasta do repositório:

```powershell
python -m pip install -r requirements-local.txt
```

## Execução

```powershell
python -B run_local.py
```

No Windows, também pode abrir `rodar_local.bat`. Ele mantém a janela aberta para
mostrar o resultado ou uma eventual mensagem de erro.

Por padrão, são usados `recovery/fornecedores.xlsx` e `recovery/admins.xlsx`, e o
resultado é salvo em `local-output/gold_kpis.json`. Os caminhos padrão são relativos
ao script, independentemente da pasta em que o comando seja executado.

Para usar outras cópias locais:

```powershell
python -B run_local.py --fornecedores "C:\dados\fornecedores.xlsx" --admins "C:\dados\admins.xlsx" --output "C:\dados\gold_kpis.json"
```

Se o Java no Windows apresentar `Unable to establish loopback connection`, tente
uma pasta temporária curta na qual tenha permissão de escrita:

```powershell
python -B run_local.py --temp-dir C:\Windows\Temp
```

Essa configuração é aplicada somente ao processo do executor.

## Resultado

O JSON contém `metadata` e `tables`. Em `tables`, cada chave é o nome de um KPI Gold
e seu valor é uma lista de registros. KPIs sem linhas ficam como `[]`. Datas e
timestamps são textos ISO 8601, valores decimais são textos para preservar a precisão,
inteiros continuam números e valores ausentes ficam como `null`.

O fuso de cálculo local é `America/Sao_Paulo`. Indicadores baseados na data atual
dependem do dia em que o executor for usado. O arquivo final é substituído somente
depois de todos os KPIs terem sido calculados com sucesso. Uma falha deixa intacto
um JSON anterior, que pode conter resultados de uma execução antiga; confira a
data `metadata.generated_at` e a mensagem de conclusão no terminal.

As mensagens sobre registros retirados pela Silver aparecem no terminal. O executor
não cria a tabela de qualidade de dados, cuja decisão ficou para uma etapa posterior.

## Alcance do teste

O executor não inicia jobs no Databricks, não escreve tabelas Delta e não chama a API
de exportação. O notebook `exports/trigger_gold_sync.py` não faz parte da execução
local, porque sua finalidade é justamente chamar essa API.

A saída inclui todos os fornecedores presentes nos arquivos locais. O filtro de
acesso por identidade do Databricks não é aplicado localmente. Os arquivos gerados
em `local-output/` são ignorados pelo Git.

A leitura local representa as células como texto antes dos tratamentos Silver e
usa os valores armazenados das fórmulas do Excel, sem recalculá-las. Isso testa as
transformações com essas entradas, mas não reproduz a inferência de tipos do leitor
nativo de Excel, permissões, row filters, escrita Delta ou configurações do workspace.
