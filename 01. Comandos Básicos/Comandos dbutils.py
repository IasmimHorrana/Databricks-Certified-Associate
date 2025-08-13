# Databricks notebook source
# MAGIC %md
# MAGIC ##### Volumes e dbutils 
# MAGIC No Databricks, a transição do uso de montagens DBFS (Databricks File System) para Volumes no Unity Catalog é recomendada para gerenciar dados não tabulares. Os volumes oferecem uma abordagem mais governada e estruturada para armazenamento e acesso a dados no Unity Catalog.
# MAGIC
# MAGIC https://docs.databricks.com/aws/en/files/volumes
# MAGIC
# MAGIC Referência de Utilitários Databricks (dbutils)
# MAGIC
# MAGIC https://learn.microsoft.com/pt-pt/azure/databricks/dev-tools/databricks-utils
# MAGIC

# COMMAND ----------

"""
Função: Lista todas as classes e métodos disponíveis dentro do dbutils.

Notas para estudo/exame:
- Mostra de forma hierárquica os módulos (fs, widgets, notebook, secrets, etc.).
- Útil quando você não lembra o nome exato de um método.
- Não retorna documentação detalhada, apenas a lista de métodos.
"""

dbutils.help()

# COMMAND ----------

"""
Função: Lista apenas os métodos do módulo fs (File System).
O fs é usado para manipular arquivos no DBFS (Databricks File System).

Notas para estudo/exame:
- Métodos comuns: ls, cp, mv, rm, put, head.
- Sempre começa com / no caminho do arquivo/diretório.
"""

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Traduzido
# MAGIC cp: Copia arquivo/diretório para outro local (recursivo opcional).
# MAGIC
# MAGIC head: Retorna os primeiros bytes de um arquivo como string UTF-8.
# MAGIC
# MAGIC ls: Lista o conteúdo de um diretório.
# MAGIC
# MAGIC mkdirs: Cria diretório, incluindo diretórios-pai se necessário.
# MAGIC
# MAGIC mv: Move arquivo/diretório para outro local (recursivo opcional).
# MAGIC
# MAGIC put: Escreve string em um arquivo como UTF-8 (sobrescrita opcional).
# MAGIC
# MAGIC rm: Remove arquivo/diretório (recursivo opcional).
# MAGIC
# MAGIC "Recursivo" significa que a operação será aplicada de forma repetitiva a todos os itens dentro de uma estrutura, como subdiretórios e arquivos contidos em um diretório.

# COMMAND ----------

"""
Função: Lista o conteúdo do diretório raiz / no DBFS.

Retorna um DataFrame com colunas path, name e size.
O display() renderiza o resultado de forma visual (tabela) no notebook do Databricks.

Notas para estudo/exame:
- ls é equivalente a "listar diretório" no terminal (ls no Linux).
- display() não é obrigatório; sem ele, o Spark retorna o resultado no formato padrão do Python.
- No exame, saiba diferenciar display() (formatação visual no Databricks) de show() (método Spark).
"""

display(dbutils.fs.ls('/'))

# COMMAND ----------

# O caminho /Volumes/ é usado no Unity Catalog para acessar dados governados.
# A estrutura /Volumes/<catalog>/<schema>/<table> é padrão no Unity Catalog.

display(dbutils.fs.ls('/Volumes/workspace/default/datasets'))

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/workspace/default/datasets/criando_pasta')

# COMMAND ----------

# Renomeando ou movendo arquivos
# Quando a origem e o destino estão no mesmo diretório pai, você está renomeando a pasta/arquivo.
# Quando a origem e o destino estão em diretórios diferentes, você está movendo a pasta/arquivo para outro local.

dbutils.fs.mv('/Volumes/workspace/default/datasets/criando_pasta','/Volumes/workspace/default/datasets/criando_pasta_nova/',True)

# COMMAND ----------

# Listando o que tem dentro da pasta Dados IBGE

display(dbutils.fs.ls('/Volumes/workspace/default/datasets/Dados-IBGE/'))

# COMMAND ----------

bikeSharing_arquivo ='/Volumes/workspace/default/datasets/bikeSharing/data-001/day.csv'

df= spark.read.csv(
    bikeSharing_arquivo,
    header=True,
    inferSchema=True,
    sep=',')

display(df)

# COMMAND ----------

spark.read.csv  # Lendo arquivos CSV com Spark
header=True # Indica que a primeira linha do CSV contém os nomes das colunas.
inferSchema=True # Faz o Spark inferir automaticamente o tipo de dados de cada coluna (int, string, float, etc.).
sep=',' # Define o separador entre os valores do CSV (vírgula por padrão).

# COMMAND ----------

arquivo = '/Volumes/workspace/default/datasets/bikeSharing/data-001/day.csv'
dbutils.fs.rm(arquivo) # removendo arquivo
