# Databricks notebook source
# DBTITLE 1,Estabelecendo conexão com o data lake
config = {"fs.azure.account.key.{container}.blob.core.windows.net":dbutils.secrets.get(scope = "{scope}", key = "{key_vault")}

# COMMAND ----------

# DBTITLE 1,Lista de containers
containers = ["raw", "processed", "lookup"]

# COMMAND ----------

# DBTITLE 1,Criar ponto de montagem nas camadas
def mount_datalake(containers):
    try:
        for container in containers:
            dbutils.fs.mount(
                source = f"wasbs://{container}@{container}.blob.core.windows.net",
                mount_point = f"/mnt/{container}",
                extra_configs = config
            )
    except ValueError as err:
        print(err)

# COMMAND ----------

# DBTITLE 1,Desmonta camadas do DBFS - Alerta (Apaga camadas)
def unmount_datalake(containers):
    try:
        for container in containers:
            dbutils.fs.unmount(f"/mnt/{container}/")
    except ValueError as err:
        print(err)

# COMMAND ----------

# DBTITLE 1,Lista camadas - DBFS
# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/

# COMMAND ----------

# DBTITLE 1,Lista camadas - DBFS
dbutils.fs.ls ("/mnt/")

# COMMAND ----------

# DBTITLE 1,Validação Key Vault
dbwestudosbruno = "HJHthyuiop245#$%¨&*()_"
print(dbwestudosbruno)

dbwlakehousekey = dbutils.secrets.get(scope = "{scope}", key = "{key_vault}")
print("teste/"+dbwlakehousekey)
