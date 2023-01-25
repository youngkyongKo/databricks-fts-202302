-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="7aa87ebc-24dd-4b39-bb02-7c59fa083a14"/>
-- MAGIC 
-- MAGIC # Managing Delta Tables
-- MAGIC <br>
-- MAGIC 이 노트북에서는 SparkSQL 을 사용해서 아래와 같이 Delta Lake 형식의 데이터를 다루는 다양한 방법에 대해서 다룹니다. 
-- MAGIC 
-- MAGIC * Delta Lake 테이블 생성 
-- MAGIC * Delta Lake 테이블의 데이터 조회 
-- MAGIC * Insert, Update, Delete 등 다양한 DML문으로 데이터를 처리 
-- MAGIC * MERGE 구문을 이용한 Upsert 처리 
-- MAGIC * Delta Lake 테이블 삭제

-- COMMAND ----------

-- MAGIC %md <i18n value="add37b8c-6a95-423f-a09a-876e489ef17d"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## 실습 환경 셋업
-- MAGIC <br>
-- MAGIC 먼저 아래 setup 스크립트를 실행합니다. 이 스크립트는 username, userhome, database 등 실습 환경을 세팅합니다.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-02.1

-- COMMAND ----------

-- MAGIC %md <i18n value="3b9c0755-bf72-480e-a836-18a4eceb97d2"/>
-- MAGIC 
-- MAGIC ## Delta Table 생성
-- MAGIC 
-- MAGIC Delta Lake 테이블을 생성하는 방법은 여러가지가 있습니다. 가장 쉬운 방법으로, CREATE TABLE 구문을 이용하여 빈 테이블을 생성해 봅시다.
-- MAGIC 
-- MAGIC **NOTE:** Databricks Runtime 8.0 이상의 버전에서는 Delta Lake 가 디폴트 포맷이므로 **`USING DELTA`** 를 생략할 수 있습니다.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS students 
  (id INT, name STRING, value DOUBLE)

-- COMMAND ----------

-- MAGIC %md <i18n value="408b1c71-b26b-43c0-b144-d5e92064a5ac"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Delta Table에 데이터 입력
-- MAGIC 일반적으로 테이블에 데이터를 입력할 때는 다른 데이터셋 쿼리 결과를 이용하여 입력하는 경우가 많습니다. 
-- MAGIC 
-- MAGIC 하지만 아래와 같이 INSERT 구문을 사용하여 직접 입력하는 것도 가능합니다.  

-- COMMAND ----------

INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

-- COMMAND ----------

-- MAGIC %md <i18n value="853dd803-9f64-42d7-b5e8-5477ea61029e"/>
-- MAGIC 
-- MAGIC 위의 셀에서는 3건의 **`INSERT`** 구문을 각각 실행했습니다. 각각의 구문은 ACID가 보장된 별개의 트랜잭션으로 처리됩니다. 
-- MAGIC 
-- MAGIC 아래와 같이 한 번의 트랜잭션에 여러 레코드를 입력할 수 있습니다. 

-- COMMAND ----------

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3)

-- COMMAND ----------

-- MAGIC %md <i18n value="121bd36c-10c4-41fc-b730-2a6fb626c6af"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Delta Table 조회

-- COMMAND ----------

SELECT * FROM students ORDER BY id 

-- COMMAND ----------

-- MAGIC %md <i18n value="4ecaf351-d4a4-4803-8990-5864995287a4"/>
-- MAGIC 
-- MAGIC Delta Lake 는 **항상** 해당 테이블의 가장 최신 버전 데이터를 읽어 오며, 진행중인 다른 작업에 영향 받지 않습니다. 

-- COMMAND ----------

-- MAGIC %md <i18n value="8a379d8d-7c48-43b0-8e25-3e653d8d6e86"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## 레코드 UPDATE
-- MAGIC 
-- MAGIC Delta Lake를 사용하면 마치 Database를 사용하는 것처럼 Insert,Update,Delete를 사용해서 손쉽게 데이터셋을 수정할 수 있습니다. 
-- MAGIC 
-- MAGIC UPDATE 작업도 ACID 트랜잭션이 보장됩니다. 

-- COMMAND ----------

UPDATE students 
SET value = value + 1
WHERE name LIKE "T%"

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md <i18n value="d581b9a2-f450-43dc-bff3-2ea9cc46ad4c"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## 레코드 DELETE
-- MAGIC 
-- MAGIC DELETE 작업도 역시 ACID 트랜잭션이 보장됩니다. 즉, 데이터의 일부만 삭제되어 일관성이 깨지는 것을 걱정할 필요가 없습니다.
-- MAGIC 
-- MAGIC DELETE 구문에 의해 하나 또는 여러 건의 레코드가 삭제될 수 있지만, 이는 항상 단일 트랜잭션 안에서 처리됩니다. 

-- COMMAND ----------

DELETE FROM students 
WHERE value > 6

-- COMMAND ----------

-- MAGIC %md <i18n value="b5b346b8-a3df-45f2-88a7-8cf8dea6d815"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## MERGE 를 이용한 Upsert 
-- MAGIC 
-- MAGIC Databricks에서는 **MERGE** 문을 이용하여 upsert (데이터의 Update, Insert 및 기타 데이터 조작을 하나의 명령어로 수행)를 처리할 수 있습니다. 
-- MAGIC 
-- MAGIC 아래의 예제에서는, 변경사항을 기록하는 CDC(Change Data Capture) 로그 데이터를 updates라는 임시뷰로 생성합니다. 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
SELECT * FROM updates;

-- COMMAND ----------

-- MAGIC %md <i18n value="6fe009d5-513f-4b93-994f-1ae9a0f30a80"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 이 view에는 레코드들에 대한 3가지 타입- insert,update,delete 명령어 기록을 담고 있습니다.  
-- MAGIC 이 명령어를 각각 수행한다면 3개의 트렌젝션이 되고 만일 이중에 하나라도 실패하게 된다면 invalid한 상태가 될 수 있습니다.  
-- MAGIC 대신에 이 3가지 action을 하나의 atomic 트랜잭션으로 묶어서 한꺼번에 적용되도록 합니다.  
-- MAGIC <br>
-- MAGIC **`MERGE`**  문은 최소한 하나의 기준 field (여기서는 id)를 가지고 각 **`WHEN MATCHED`** 이나 **`WHEN NOT MATCHED`**  구절은 여러 조건값들을 가질 수 있습니다.  
-- MAGIC **id** 필드를 기준으로 **type** 필드값에 따라서 각 record에 대해서 update,delete,insert문을 수행하게 됩니다. 

-- COMMAND ----------

MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *

-- COMMAND ----------

-- MAGIC %md <i18n value="4eca2c53-e457-4964-875e-d39d9205c3c6"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## 테이블 DROP
-- MAGIC 
-- MAGIC **`DROP TABLE`** 구문을 이용하여 테이블을 삭제할 수 있습니다. 

-- COMMAND ----------

DROP TABLE students

-- COMMAND ----------

-- MAGIC %md <i18n value="08cbbda5-96b2-4ae8-889f-b1f4c04d1496"/>
-- MAGIC 
-- MAGIC ## 실습 환경 정리
-- MAGIC 
-- MAGIC 아래 셀을 실행하여 실습용으로 생성한 테이블과 타일들을 정리합니다.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
