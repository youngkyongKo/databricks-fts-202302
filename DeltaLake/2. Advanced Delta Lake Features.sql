-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="d2b35611-0c56-4262-b664-3a89a1d62662"/>
-- MAGIC 
-- MAGIC 
-- MAGIC # Advanced Delta Lake Features
-- MAGIC 
-- MAGIC Delta Lake 테이블이 제공하는 고유한 기능들을 살펴 보겠습니다.  
-- MAGIC 
-- MAGIC * **`OPTIMIZE`** 구문을 이용하여 자잘한 파일들을 적절한 크기로 최적화 (compaction) 
-- MAGIC * **`ZORDER`**  구문을 이용한 테이블 인덱싱
-- MAGIC * Delta Lake 테이블의 디렉토리 구조 살펴보기
-- MAGIC * 테이블 트랜잭션 이력 조회
-- MAGIC * 테이블의 이전 버전 데이터를 조회하거나 해당 버전으로 롤백
-- MAGIC * **`VACUUM`** 을 이용하여 오래된 데이터 정리 
-- MAGIC 
-- MAGIC **참조문서**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Delta Optimize - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">Delta Vacuum - Databricks Docs</a>

-- COMMAND ----------

-- MAGIC %md <i18n value="75224cfc-51b5-4c3d-8eb3-4db08469c99f"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## 실습 환경 설정

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-02.3

-- COMMAND ----------

-- MAGIC %md <i18n value="7e85feea-be41-41f7-9cd7-df2c140d6286"/>
-- MAGIC 
-- MAGIC ## Delta Lake 테이블 생성 후 데이터 작업
-- MAGIC 
-- MAGIC 이전 노트북에서 수행했던 작업들을 다시 실행해 봅시다.

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md <i18n value="5f6b0330-42f2-4307-9ff2-0b534947b286"/>
-- MAGIC 
-- MAGIC ## 테이블 정보 살펴보기 
-- MAGIC 
-- MAGIC database, table, view 에 대한 정보는 디폴트로 Hive metastore 에 저장됩니다. 
-- MAGIC 
-- MAGIC **`DESCRIBE EXTENDED`** 구문을 이용하여 테이블의 metadata 를 살펴 봅시다. 

-- COMMAND ----------

DESCRIBE EXTENDED students

-- COMMAND ----------

-- MAGIC %md <i18n value="5495f382-2841-4cf5-b872-db4dd3828ee5"/>
-- MAGIC 
-- MAGIC 다른 방법으로, **`DESCRIBE DETAIL`** 구문을 이용하여 테이블 metadata 를 조회할 수 있습니다. 

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %md <i18n value="4ab0fa4f-72cb-4f3b-8ea3-228b13be1baf"/>
-- MAGIC 
-- MAGIC **`Location`** 필드에서 해당 테이블을 구성하는 파일들의 저장 경로를 확인할 수 있습니다. 

-- COMMAND ----------

-- MAGIC %md <i18n value="10e37764-bbfd-4669-a967-addd58041d47"/>
-- MAGIC 
-- MAGIC ## Delta Lake 파일들 살펴보기 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %md <i18n value="075483eb-7ddd-46ef-bbb1-33ee7005923b"/>
-- MAGIC 
-- MAGIC 테이블 저장 경로 디렉토리에는 parquet 포맷의 데이터 파일들과 **`_delta_log`** 디렉토리가 있습니다.  
-- MAGIC 
-- MAGIC Delta Lake 테이블의 레코드들은 parquet 파일로 저장됩니다. 
-- MAGIC 
-- MAGIC Delta Lake 테이블의 트랜잭션 기록들은 **`_delta_log`** 디렉토리 아래에 저장됩니다. 이 디렉토리를 좀 더 살펴 봅시다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %md <i18n value="1bcbb8d1-f871-451a-ad16-762dfa91c0a3"/>
-- MAGIC 
-- MAGIC 각각의 트랜잭션 로그들은 버전별 JSON 파일로 저장됩니다. 여기서는 8개의 트랜잭션 로그 파일을 볼 수 있습니다. (버전은 0부터 시작합니다) 

-- COMMAND ----------

-- MAGIC %md <i18n value="c2fbd6d7-ea8e-4000-9702-e21408f3ef78"/>
-- MAGIC 
-- MAGIC ## 데이터 파일 살펴보기
-- MAGIC 
-- MAGIC students 테이블은 매우 작은 테이블이지만 많은 수의 데이터 파일로 이루어져 있습니다. 
-- MAGIC 
-- MAGIC **`DESCRIBE DETAIL`** 구문으로 파일 갯수(numFiles)를 살펴 봅시다. 

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %md <i18n value="adf1dc55-37a4-4376-86df-78895bfcf6b8"/>
-- MAGIC 
-- MAGIC metadata 조회 결과, students 테이블의 현재 버전에는 4개의 데이터 파일이 있습니다 (numFiles=4). 그럼 테이블 디렉토리에 있는 다른 parquet 파일들은 무엇일까요?  
-- MAGIC 
-- MAGIC Delta Lake는 변경된 데이터를 담고 있는 파일들을 overwrite 하거나 즉시 삭제하지 않고, 해당 버전에서 유효한 데이터 파일들을 트랜잭션 로그에 기록합니다.     
-- MAGIC 
-- MAGIC 예를 들어 위의 **`MERGE`** 구문이 수행된 트랜잭션 로그를 살펴 봅시다. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))

-- COMMAND ----------

-- MAGIC %md <i18n value="85e8bce8-c168-4ac6-9835-f694cab5b43c"/>
-- MAGIC 
-- MAGIC **`add`** 컬럼에는 테이블에 새롭게 추가된 파일들의 정보가 담겨 있고, **`remove`** 컬럼에는 이 트랜잭션으로 삭제 처리된 파일들이 표시되어 있습니다.
-- MAGIC 
-- MAGIC Delta Lake 테이블을 쿼리할 때, 쿼리엔진은 이 트랜잭션 로그를 이용하여 현재 버전에서 유효한 파일들을 알아내고 그 외의 데이터 파일들은 무시합니다.

-- COMMAND ----------

-- MAGIC %md <i18n value="c69bbf45-e75e-419f-a149-fd18f76daab6"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## 작은 파일들의 최적화 (Compaction) 과 인덱싱
-- MAGIC 
-- MAGIC 이런저런 작업을 하다 보면 작은 데이터 파일들이 많이 생성되는 경우가 종종 발생합니다. 이와 같이 작은 파일들이 많은 경우 처리 성능 저하의 원인이 될 수 있습니다.
-- MAGIC 
-- MAGIC **`OPTIMIZE`** 명령어는 기존의 데이터 파일내의 레코드들을 합쳐서 새로 최적의 사이즈로 파일을 만들고, 기존의 작은 파일들을 읽기 성능이 좋은 큰 파일들로 대체합니다.
-- MAGIC 
-- MAGIC 이 때 하나 이상의 필드를 지정해서 **`ZORDER`** 인덱싱을 함께 수행할 수 있습니다.
-- MAGIC 
-- MAGIC Z-Ordering은 관련 정보를 동일한 파일 집합에 배치하여, 쿼리 실행시 읽어야 하는 데이터의 양을 줄여 성능을 향상 시키는 기술입니다. 쿼리 조건에 자주 사용되고 해당 열에 높은 카디널리티(distinct 값이 많은)가 있는 경우 ZORDER BY를 사용하면 효과적입니다.

-- COMMAND ----------

OPTIMIZE students
ZORDER BY id

-- COMMAND ----------

-- MAGIC %md <i18n value="5684dfb4-0b33-49f1-a4f8-cb2f8d88bf09"/>
-- MAGIC 
-- MAGIC ## Time Travel
-- MAGIC 
-- MAGIC Delta Lake 테이블의 모든 변경 이력은 트랜잭션 로그에 저장되므로, **`DESCRIBE HISTORY`** 구문을 통해 <a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">table history</a> 를 쉽게 조회할 수 있습니다.

-- COMMAND ----------

DESCRIBE HISTORY students

-- COMMAND ----------

-- MAGIC %md <i18n value="56de8919-b5d0-4d1f-81d8-ccf22fdf6da0"/>
-- MAGIC 
-- MAGIC **`OPTIMIZE`** 명령에 의해 새로운 버전(version 8)이 추가되어 students 테이블의 최신 버전이 된 것을 볼 수 있습니다. 
-- MAGIC 
-- MAGIC 트랜잭션 로그에서 removed 로 마킹된 예전 데이터 파일들이 삭제되지 않고 남아 있으므로, 이를 이용하여 테이블의 과거 버전 데이터를 조회할 수 있습니다. 아래와 같이 버전 번호나 timestamp를 지정하여 Time travel을 수행할 수 있습니다.  

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 3

-- SELECT * FROM students@v3;
-- SELECT * FROM students TIMESTAMP AS OF '2023-02-01 00:05:00';

-- COMMAND ----------

-- MAGIC %md <i18n value="0499f01b-7700-4381-80cc-9b4fb093017a"/>
-- MAGIC 
-- MAGIC Time travel 은 현재 버전에서 트랜잭션을 undo 하거나 과거 상태의 데이터를 다시 생성하는 방식이 아니라, 트랜잭션 로그를 이용하여 해당 버전에서 유효한 파일들을 찾아낸 후, 이들을 쿼리하는 방식으로 이루어 집니다.

-- COMMAND ----------

-- MAGIC %md <i18n value="f569a57f-24cc-403a-88ab-709b4f1a7548"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## 과거 버전으로 Rollback 하기

-- COMMAND ----------

-- 실수로 모든 레코드를 삭제한 상황을 가정해 봅시다.
DELETE FROM students

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md <i18n value="0477fb25-7248-4552-98a1-ffee4cd7b5b0"/>
-- MAGIC 
-- MAGIC 삭제 커밋이 수행되기 이전 버전으로 rollback 해 봅시다.

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8 

-- COMMAND ----------

-- MAGIC %md <i18n value="4fbc3b91-8b73-4644-95cb-f9ca2f1ac6a3"/>
-- MAGIC 
-- MAGIC 테이블 히스토리를 조회(DESCRIBE HISTORY)해 보면 <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">RESTORE</a> 명령이 트랜잭션으로 기록됨을 볼 수 있습니다.

-- COMMAND ----------

-- MAGIC %md <i18n value="789ca5cf-5eb1-4a81-a595-624994a512f1"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Stale File 정리하기
-- MAGIC 
-- MAGIC Delta Lake의 Versioning과 Time Travel은 과거 버전을 조회하고 실수했을 경우 데이터를 rollback하는 매우 유용한 기능이지만, 데이터 파일의 모든 버전을 영구적으로 저장하는 것은 스토리지 비용이 많이 들게 됩니다. 또한 개인정보 관련 규정에 따라 데이터를 명시적으로 삭제해야 할 경우도 있습니다. 
-- MAGIC 
-- MAGIC **`VACUUM`** 을 이용하여 Delta Lake Table 에서 불필요한 데이터 파일들을 정리할 수 있습니다. 
-- MAGIC 
-- MAGIC **`VACUUM`** 명령을 수행하면 지정한 retention 기간 이전으로 더이상 여행할 수 없게 됩니다.  

-- COMMAND ----------

-- VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md <i18n value="6a3b0b37-1387-4b41-86bf-3f181ddc1562"/>
-- MAGIC 
-- MAGIC 기본값으로 VACUUM 은 7일 미만의 데이터를 삭제하지 못하도록 설정되어 있으며, 이는 아직 사용중이거나 커밋되지 않은 파일이 삭제되어 데이터가 손상되는 것을 방지하기 위함입니다. 
-- MAGIC 
-- MAGIC 아래의 예제는 이 기본 설정을 무시하고 가장 최근 버전 데이터만 남기고 모든 과거 버전의 stale file을 정리하는 예제입니다.

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

-- DRY RUN 을 이용하여 VACUUM 실행시 삭제될 파일들을 미리 파악할 수 있습니다.   
VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- VACUUM 을 실제 실행합니다.
VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md <i18n value="a847e55a-0ecf-4b10-85ab-5aa8566ff4e1"/>
-- MAGIC 
-- MAGIC 테이블 디렉토리에서 데이터 파일들이 정상적으로 삭제되었는지 확인해 봅시다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %md <i18n value="b854a50f-635b-4cdc-8f18-38c5ab595648"/>
-- MAGIC 
-- MAGIC ## 실습 환경 정리

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Summary
-- MAGIC 
-- MAGIC Delta Lake 는 Cloud storage 기반의 데이터 레이크 환경에서 데이터 신뢰성 보장과 성능 향상의 메커니즘을 제공합니다.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
