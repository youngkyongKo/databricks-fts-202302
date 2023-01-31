# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="c84bb70e-0f3a-4cb9-a8b4-882200c7c940"/>
# MAGIC 
# MAGIC 
# MAGIC # Incremental Multi-Hop in the Lakehouse
# MAGIC 
# MAGIC 이 노트북에서는 Spark Structured Streaming 과 Delta Lake를 사용해서 통합된 Multi Hop 파이프라인에서 손쉽게 streaming 과 batch workload를 수행하는 방법에 대해서 다룹니다.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 이 단원을 마치면 다음을 수행할 수 있습니다.
# MAGIC * 브론즈, 실버, 골드 테이블 설명
# MAGIC * Delta Lake Multi-Hop 파이프라인 생성

# COMMAND ----------

# MAGIC %md <i18n value="8f7d994a-fe1f-4628-825e-30c35b9ff187"/>
# MAGIC 
# MAGIC 
# MAGIC ## Incremental Updates in the Lakehouse
# MAGIC 
# MAGIC Delta Lake를 사용하면 사용자가 통합된 Multi-Hop 파이프라인에서 스트리밍 및 배치 워크로드를 쉽게 결합할 수 있습니다. 파이프라인의 각 단계는 비즈니스 내에서 핵심 use case를 추진하는 데 중요한 데이터 상태를 나타냅니다. 모든 데이터와 메타데이터는 클라우드의 객체 스토리지에 있기 때문에 여러 사용자와 애플리케이션이 준 실시간(near-real time)으로 데이터에 액세스할 수 있으므로 분석가는 처리 중인 최신 데이터에 액세스할 수 있습니다.
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/sslh/multi-hop-simple.png)
# MAGIC 
# MAGIC - **Bronze** 테이블은 다양한 소스((JSON files, RDBMS data,  IoT data등)에서 수집한 원본 데이터를 저장합니다. 
# MAGIC 
# MAGIC - **Silver** 테이블은 우리 데이터에 좀 더 정제된 view를 제공합니다. 다양한 bronze 테이블과 조인하거나 불필요한 정보의 제거, 업데이트 등을 수행합니다. 
# MAGIC 
# MAGIC - **Gold** 테이블은 주로 리포트나 대시보드에서 사용되는 비지니스 수준의 aggregation을 수행한 뷰를 제공합니다.  일간사용자수나 상품별 매출등의 뷰가 이 예입니다. 
# MAGIC 
# MAGIC 최종 Output은 실행 가능한 insight, 대시보드 및 비즈니스 지표 보고서입니다.
# MAGIC 
# MAGIC ETL 파이프라인의 모든 단계에서 비즈니스 logic을 고려함으로써 불필요한 데이터 중복을 줄이고 전체 히스토리 데이터에 대한 adhoc 쿼리를 제한하여 스토리지 및 컴퓨팅 비용을 최적화할 수 있습니다.
# MAGIC 
# MAGIC 각 단계는 배치 또는 스트리밍 작업으로 구성할 수 있으며 ACID 트랜잭션은 작업이 완전히 성공하거나 실패하는지를 보장합니다.

# COMMAND ----------

# MAGIC %md <i18n value="9008b325-00b1-41a3-bc43-9c693bade882"/>
# MAGIC 
# MAGIC 
# MAGIC ## Datasets Used
# MAGIC 
# MAGIC 이 데모는 인위적으로 생성된 단순화된 의료 데이터를 사용합니다. 두 데이터 세트의 스키마는 다음과 같습니다. 다양한 단계에서 해당 스키마를 다뤄 보도록 할 것입니다.
# MAGIC 
# MAGIC #### Recordings
# MAGIC 기본 데이터 세트는 JSON 형식으로 제공되는 의료 기기의 심박수 데이터를 사용합니다.
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | device_id | int |
# MAGIC | mrn | long |
# MAGIC | time | double |
# MAGIC | heartrate | double |
# MAGIC 
# MAGIC #### PII
# MAGIC PII 데이터는 나중에 이름으로 환자를 식별하기 위해 외부 시스템에 저장된 환자 정보의 테이블과 조인 됩니다.
# MAGIC 
# MAGIC | Field | Type |
# MAGIC | --- | --- |
# MAGIC | mrn | long |
# MAGIC | name | string |

# COMMAND ----------

# MAGIC %md <i18n value="7b621659-663d-4fea-b26c-5eefdf4d025a"/>
# MAGIC 
# MAGIC 
# MAGIC ## Getting Started
# MAGIC 
# MAGIC 다음 셀을 실행하여 랩 환경을 구성하십시오.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-07.1

# COMMAND ----------

# MAGIC %md <i18n value="045c9907-e803-4506-8e69-4e370f06cd1d"/>
# MAGIC 
# MAGIC 
# MAGIC ## Data Simulator
# MAGIC Databricks Auto Loader는 파일이 클라우드 Object Storage 에 도착하면 자동으로 파일을 읽어 처리할 수 있습니다.
# MAGIC 
# MAGIC 시뮬레이션을 위해서 다음 데이터를 로드하는 작업을 여러 번 실행하라는 메시지가 표시됩니다.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md <i18n value="d5d9393e-0a91-41f5-95f3-82f1be290add"/>
# MAGIC 
# MAGIC 
# MAGIC ## Bronze Table: Ingesting Raw JSON Recordings
# MAGIC 
# MAGIC 아래에서는 스키마 추론과 함께 Auto Loader를 사용하여 Raw 데이터인 JSON 소스에 대한 읽기를 구성합니다.
# MAGIC 
# MAGIC **참고**: JSON 데이터 소스의 경우 Auto Loader는 기본적으로 각 열을 문자열로 유추합니다. 여기서는 **`cloudFiles.schemaHints`** 옵션을 사용하여 **`time`** 열의 데이터 유형을 지정하는 방법을 보여줍니다. 필드에 대해 부적절한 타입을 지정하면 null 값이 생성됩니다.

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaHints", "time DOUBLE")
    .option("cloudFiles.schemaLocation", f"{DA.paths.checkpoints}/bronze")
    .load(DA.paths.data_landing_location)
    .createOrReplaceTempView("recordings_raw_temp"))

# COMMAND ----------

# MAGIC %md <i18n value="7fdec7ea-277e-4df4-911b-b2a4d3761b6a"/>
# MAGIC 
# MAGIC 
# MAGIC 여기에서는 원본 파일과 수집된 시간을 메타데이터로 추가하여 Raw 데이터를 보강 합니다. 이 추가 메타데이터는 다운스트림 처리 중에 무시할 수 있으며 손상된 데이터가 발생하는 경우 오류 문제 해결에 유용한 정보를 제공합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recordings_bronze_temp AS (
# MAGIC   SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM recordings_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %md <i18n value="6f60f3aa-65ff-4204-9cf8-00f456d4497b"/>
# MAGIC 
# MAGIC 
# MAGIC 아래 코드는 보강된 Raw 데이터를 PySpark API로 다시 전달하여 bronze 라는 이름의 Delta Lake 테이블에 대한 증분 쓰기를 처리합니다.

# COMMAND ----------

(spark.table("recordings_bronze_temp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")
      .outputMode("append")
      .table("bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from bronze;

# COMMAND ----------

# MAGIC %md <i18n value="6fd28dc4-1516-4f6a-8478-290d366a342c"/>
# MAGIC 
# MAGIC 
# MAGIC 다음 셀을 사용하여 다른 파일을 로드하면 작성한 스트리밍 쿼리에서 즉시 감지된 변경 사항을 볼 수 있습니다.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from bronze;

# COMMAND ----------

# MAGIC %md <i18n value="4d7848cc-ecff-474d-be27-21717d9f08d1"/>
# MAGIC 
# MAGIC 
# MAGIC ### Load Static Lookup Table
# MAGIC 
# MAGIC ACID는 Delta Lake가 처리한 데이터가 테이블 레벨에서 관리되고 완결성이 보장되어서 완전히 성공한 커밋만 테이블에 반영되도록 합니다. 데이터를 다른 데이터의 원본과 Merge 하기로한 경우, 해당 원본의 데이터의 버전과 일관성이 보장되어야 하는 것을 알고 있어야 합니다.
# MAGIC 
# MAGIC 이 데모에서는 **Recordings** 에 환자 데이터(pii)를 추가하기 위해 CSV 파일을 로드합니다. 프로덕션에서는 Databricks의 <a href="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> 기능을 사용할 수 있습니다. Delta Lake에서 이를 통해 가장 신선한 최신 데이터를 볼 수 있습니다.

# COMMAND ----------

(spark.read
      .format("csv")
      .schema("mrn STRING, name STRING")
      .option("header", True)
      .load(f"{DA.paths.datasets}/healthcare/patient/patient_info.csv")
      .createOrReplaceTempView("pii"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pii

# COMMAND ----------

# MAGIC %md <i18n value="00ed3fc7-7c17-44f0-b56f-5e824a72bd9c"/>
# MAGIC 
# MAGIC 
# MAGIC ## Silver Table: Enriched Recording Data
# MAGIC 실버 레벨의 두 번째 레이어로 다음 데이터의 대한 enrichment 및 check를 수행합니다.
# MAGIC - recordings 데이터는 환자 이름을 추가하기 위해 PII와 조인 됩니다.
# MAGIC - recordings 시간은 사람이 읽을 수 있도록 **`'yyyy-MM-dd HH:mm:ss'`** 형식으로 구문 분석됩니다.
# MAGIC - <= 0인 심박수는 환자 부재 또는 전송 오류를 나타내므로 제외합니다.

# COMMAND ----------

(spark.readStream
  .table("bronze")
  .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW recordings_w_pii AS (
# MAGIC   SELECT device_id, a.mrn, b.name, cast(from_unixtime(time, 'yyyy-MM-dd HH:mm:ss') AS timestamp) time, heartrate
# MAGIC   FROM bronze_tmp a
# MAGIC   INNER JOIN pii b
# MAGIC   ON a.mrn = b.mrn
# MAGIC   WHERE heartrate > 0)

# COMMAND ----------

(spark.table("recordings_w_pii")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/recordings_enriched")
      .outputMode("append")
      .table("recordings_enriched"))

# COMMAND ----------

# MAGIC %md <i18n value="f7f66dc8-f5b5-4682-bfb6-e97aab650874"/>
# MAGIC 
# MAGIC 
# MAGIC Trigger another new file and wait for it propagate through both previous queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM recordings_enriched

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %md <i18n value="d6a2ecd9-043e-4488-8a70-3ee3389cf681"/>
# MAGIC 
# MAGIC 
# MAGIC ## Gold Table: Daily Averages
# MAGIC 
# MAGIC 여기서 우리는 **`recordings_enriched`** 에서 데이터 스트림을 읽고 또 다른 스트림을 작성하여 각 환자에 대한 일일 평균의 집계 골드 테이블을 만듭니다.

# COMMAND ----------

(spark.readStream
  .table("recordings_enriched")
  .createOrReplaceTempView("recordings_enriched_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW patient_avg AS (
# MAGIC   SELECT mrn, name, mean(heartrate) avg_heartrate, date_trunc("DD", time) date
# MAGIC   FROM recordings_enriched_temp
# MAGIC   GROUP BY mrn, name, date_trunc("DD", time))

# COMMAND ----------

# MAGIC %md <i18n value="de6370ea-e1a0-4212-98eb-53fd012e73b0"/>
# MAGIC 
# MAGIC 
# MAGIC 아래 코드에서는 **`.trigger(availableNow=True)`** 를 사용하고 있습니다. 이를 통해 이 작업을 한 번 트리거하여 수집 가능한 모든 데이터를 마이크로 배치로 처리하면서 Structured Streaming의 강점을 계속 사용할 수 있습니다. 
# MAGIC 요약하자면 이러한 강점은 다음과 같습니다.
# MAGIC - 정확히 한 번 (exactly once) end-to-end fault tolerant processing
# MAGIC - 업스트림 데이터 소스의 변경 사항 자동 감지
# MAGIC 
# MAGIC 데이터가 증가하는 대략적인 속도를 알면 빠르고 비용 효율적인 처리를 위해 이 작업에 대해 사용할 클러스터의 크기를 적절하게 조정할 수 있습니다. 고객은 업데이트되는 최종 집계 테이블의 데이터의 비용과 양을 평가하고 이 작업을 실행해야 하는 빈도에 대해 정확한 정보에 입각한 결정을 내릴 수 있습니다.
# MAGIC 
# MAGIC 이 테이블을 구독하는 다운스트림 프로세스는 비용이 많이 드는 집계를 다시 실행할 필요가 없습니다. 오히려 파일을 de-serialize 하면 포함된 필드를 기반으로 하는 쿼리를 이미 집계된 이 소스에 대해 신속하게 푸시다운할 수 있습니다.

# COMMAND ----------

(spark.table("patient_avg")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", f"{DA.paths.checkpoints}/daily_avg")
      .trigger(availableNow=True)
      .table("daily_patient_avg"))

# COMMAND ----------

# MAGIC %md <i18n value="5ffbd353-850f-431e-8455-827f87cad2ca"/>
# MAGIC 
# MAGIC 
# MAGIC #### Important Considerations for complete Output with Delta
# MAGIC 
# MAGIC **`complete`** Output 모드를 사용하는 경우 로직이 실행될 때마다 결과 테이블의 전체 상태를 다시 rewrite 합니다. 이는 집계 계산에 이상적이지만, Structured Streaming 에서는 데이터가 업스트림 로직에만 추가된다고 가정하므로 이 디렉터리에서는 스트림을 읽을 수 **없습니다**.
# MAGIC 
# MAGIC **Note**: 특정 옵션을 설정하여 이 동작을 변경할 수 있지만 다른 제한 사항이 있습니다. 자세한 내용은 <a href="https://docs.databricks.com/delta/delta-streaming.html#ignoring-updates-and-deletes" target="_blank">Delta Streaming: Ignoring Updates and Deletes. </a>
# MAGIC 
# MAGIC 방금 등록한 **daily_patient_avg** 골드 테이블은 다음 쿼리를 실행할 때마다 데이터의 현재 상태에 대한 정적 읽기를 수행합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from daily_patient_avg;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_patient_avg

# COMMAND ----------

# MAGIC %md <i18n value="bcca7247-9716-44ed-8424-e72170f0a2dc"/>
# MAGIC 
# MAGIC 위의 테이블에는 모든 사용자의 모든 날짜가 포함되어 있습니다. Adoc 쿼리를 사용하여 특정 날짜를 predicate 하여 조회 하실 수 있습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM daily_patient_avg
# MAGIC WHERE date BETWEEN "2020-01-17" AND "2020-01-31"

# COMMAND ----------

# MAGIC %md <i18n value="0822f785-38af-4b30-9154-8d82eb9fe000"/>
# MAGIC 
# MAGIC 
# MAGIC ## Process Remaining Records
# MAGIC 
# MAGIC 다음 셀은 나머지 2020년 전체 데이터를 소스 디렉터리에 추가 로드합니다. Delta Lake에서 처음 만든 3개 테이블을 통해 이러한 프로세스를 볼 수 있지만 **`daily_patient_avg`** 테이블을 업데이트하려면 마지막 쿼리를 다시 실행해야 합니다. (**`.trigger(availableNow=True)`**) 구문을 사용하고 있기 때문에 전체 상태를 rewrite 해야 하기 때문입니다.

# COMMAND ----------

DA.data_factory.load(continuous=True)

# COMMAND ----------

# MAGIC %md <i18n value="f1f576bc-2b5d-46cf-9acb-6c7c4807c1af"/>
# MAGIC 
# MAGIC 
# MAGIC ## Wrapping Up
# MAGIC 
# MAGIC Finally, make sure all streams are stopped.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md <i18n value="82928cc5-5e2b-4368-90bd-dff62a27ff12"/>
# MAGIC 
# MAGIC 
# MAGIC ## Summary
# MAGIC 
# MAGIC Delta Lake와 Structured Streaming이 결합되어 레이크하우스의 데이터에 대한 준 실시간 분석 액세스를 제공합니다.

# COMMAND ----------

# MAGIC %md <i18n value="e60b0dac-92ed-4480-a969-d0568ce83494"/>
# MAGIC 
# MAGIC 
# MAGIC ## Additional Topics & Resources
# MAGIC 
# MAGIC * <a href="https://docs.databricks.com/delta/delta-streaming.html" target="_blank">Table Streaming Reads and Writes</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">Structured Streaming Programming Guide</a>
# MAGIC * <a href="https://www.youtube.com/watch?v=rl8dIzTpxrI" target="_blank">A Deep Dive into Structured Streaming</a> by Tathagata Das. This is an excellent video describing how Structured Streaming works.
# MAGIC * <a href="https://databricks.com/glossary/lambda-architecture" target="_blank">Lambda Architecture</a>
# MAGIC * <a href="https://bennyaustin.wordpress.com/2010/05/02/kimball-and-inmon-dw-models/#" target="_blank">Data Warehouse Models</a>
# MAGIC * <a href="http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html" target="_blank">Create a Kafka Source Stream</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
