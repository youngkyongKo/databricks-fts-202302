# Databricks notebook source
# MAGIC %md <i18n value="46a8383a-807d-42a9-9bdc-09a34e0b7bd0"/>
# MAGIC 
# MAGIC 
# MAGIC # DLT 파이프라인의 결과 탐색
# MAGIC 
# MAGIC 이 노트북은 DLT 파이프라인의 실행 결과를 탐색합니다.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-08.1.3

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="4b109d6f-b0d4-4ded-ac54-a12f722599a9"/>
# MAGIC 
# MAGIC 
# MAGIC **`system`** 디렉토리에는 파이프라인과 관련된 이벤트를 캡처합니다.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="fd83d9bb-db62-456a-8a51-33e8680c0d47"/>
# MAGIC 
# MAGIC 
# MAGIC 이러한 이벤트 로그는 델타 테이블로 저장됩니다. 테이블을 쿼리해 봅시다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${da.paths.storage_location}/system/events`

# COMMAND ----------

# MAGIC %md <i18n value="b0c12205-fc10-4a63-a73e-d5cded65ef51"/>
# MAGIC 
# MAGIC 
# MAGIC *tables* 디렉토리의 내용을 봅니다.

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# MAGIC %md <i18n value="e5072a3f-3a1f-4e4d-89fa-bda3926cb6ba"/>
# MAGIC 
# MAGIC 
# MAGIC Gold 테이블을 쿼리해 봅니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${da.schema_name}.sales_order_in_la

# COMMAND ----------

# MAGIC %md <i18n value="69333106-f421-45f0-a846-1c3d4fc8ddcb"/>
# MAGIC 
# MAGIC  
# MAGIC 다음 셀을 실행하여 이 학습과 연관된 테이블 및 파일을 삭제하십시오.

# COMMAND ----------

DA.cleanup()
