-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md <i18n value="b9ee68dd-a00f-45a6-9c7a-9c59a41fef6a"/>
-- MAGIC 
-- MAGIC 
-- MAGIC # 델타 라이브 테이블용 SQL
-- MAGIC 
-- MAGIC 이 노트북을 델타 라이브 테이블(DLT) 파이프라인으로 활용하는 과정을 살펴보았습니다. 이제 델타 라이브 테이블에서 사용하는 구문을 더 잘 이해하기 위해 이 노트북의 내용을 살펴보겠습니다.
-- MAGIC 
-- MAGIC 가장 간단하게 DLT SQL은 기존 CTAS 문을 약간 수정한 것으로 생각할 수 있습니다. DLT 테이블과 뷰는 항상 **`LIVE`** 키워드 앞에 옵니다.
-- MAGIC 
-- MAGIC ## Learning Objectives
-- MAGIC 이 단원을 마치면 다음을 수행할 수 있습니다.
-- MAGIC * 델타 라이브 테이블로 테이블 및 보기 정의
-- MAGIC * SQL을 사용하여 Auto Loader로 raw 데이터를 점진적으로 수집
-- MAGIC * SQL을 사용하여 Delta 테이블에서 증분된 읽기 수행
-- MAGIC * 코드 업데이트 및 파이프라인 재배포

-- COMMAND ----------

-- MAGIC %md <i18n value="1cc1fe77-a235-40dd-beb5-bacc94425b96"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Declare Bronze Layer Tables
-- MAGIC 
-- MAGIC 아래에서 브론즈 레이어를 구현하는 두 개의 테이블을 선언합니다. 이것은 가장 raw 한 형태의 데이터를 나타내지만, 무기한 보존할 수 있는 형식으로 캡처되고 Delta Lake가 제공해야 하는 성능과 이점으로 쿼리됩니다.

-- COMMAND ----------

-- MAGIC %md <i18n value="458afeb2-4472-4ed2-a4c7-6dea2338c3f2"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ### sales_orders_raw
-- MAGIC 
-- MAGIC **`sales_orders_raw`** 는 **retail-org/sales_orders** 데이터 세트에서 점진적으로 JSON 데이터를 수집합니다.
-- MAGIC 
-- MAGIC <a herf="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a> 를 통한 증분 처리(Structured Streaming 으로 모델과 동일한 처리 사용), 아래와 같이 선언에 **`STREAMING`** 키워드를 추가해야 합니다. **`cloud_files()`** 메서드를 사용하면 Auto Loader 를 기본적으로 SQL과 함께 사용할 수 있습니다. 이 메서드는 다음 위치 매개변수를 사용합니다.
-- MAGIC * 위에서 언급한 소스 위치
-- MAGIC * 소스 데이터 형식(이 경우 JSON)
-- MAGIC * 선택적 reader 옵션의 임의의 크기 배열입니다. 이 경우 **`cloudFiles.inferColumnTypes`**를 **`true`**로 설정합니다.
-- MAGIC 
-- MAGIC 다음 선언은 또한 데이터 카탈로그를 탐색하는 모든 사용자에게 표시되는 추가 테이블 메타데이터(이 경우 주석 및 속성)의 선언을 보여줍니다.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from retail-org/sales_orders."
AS SELECT * FROM cloud_files("${datasets_path}/retail-org/sales_orders", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md <i18n value="57accbe1-e96d-44ca-8937-8849634d1da2"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ### customers
-- MAGIC 
-- MAGIC **`customers`** 는 **retail-org/customers** 에 있는 CSV 포맷의 고객데이터를 제공합니다.
-- MAGIC 
-- MAGIC 이 테이블은 조인 작업에서 곧 사용되어 판매 기록을 기반으로 고객 데이터를 조회합니다.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers
COMMENT "The customers buying finished products, ingested from retail-org/customers."
AS SELECT * FROM cloud_files("${datasets_path}/retail-org/customers/", "csv");

-- COMMAND ----------

-- MAGIC %md <i18n value="09bc5c4d-1408-47f8-8102-ddaf0a96a6c0"/>
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Declare Silver Layer Tables
-- MAGIC 
-- MAGIC 이제 실버 레이어를 구현하는 테이블을 선언합니다. 이 레이어는 다운스트림 애플리케이션을 최적화하기 위해 브론즈 레이어의 정제된 데이터 복사본을 나타냅니다. 이 level에서는 데이터 cleansing 및 enrichment 같은 작업을 적용합니다.

-- COMMAND ----------

-- MAGIC %md <i18n value="eb20ea4d-1115-4536-adac-7bbd85491726"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ### sales_orders_cleaned
-- MAGIC 
-- MAGIC 여기서 우리는 order number 컬럼의 값이 null 인 레코드를 걸러내서 데이터의 품질 관리를 구현하여 customer 정보와 sales transaction 데이터를 enrich 합니다.
-- MAGIC 
-- MAGIC 이 선언은 많은 새로운 개념을 소개합니다.
-- MAGIC 
-- MAGIC #### Quality Control
-- MAGIC 
-- MAGIC **`CONSTRAINT`** 키워드는 품질 관리를 나타냅니다. 기존 **`WHERE`** 절과 기능이 유사한 **`CONSTRAINT`** 는 DLT와 통합되어 제약 조건 위반에 대한 메트릭을 수집할 수 있습니다. 제약 조건은 선택적 **`ON VIOLATION`** 절을 제공하여 제약 조건을 위반하는 레코드에 대해 수행할 작업을 지정합니다. 현재 DLT에서 지원하는 세 가지 모드는 다음과 같습니다.
-- MAGIC 
-- MAGIC | **`ON VIOLATION`** | Behavior |
-- MAGIC | --- | --- |
-- MAGIC | **`FAIL UPDATE`** | 제약조건 위반 시 파이프라인 실패 |
-- MAGIC | **`DROP ROW`** | 제약 조건을 위반하는 레코드 폐기 |
-- MAGIC | Omitted | 제약 조건을 위반하는 레코드가 포함됩니다(그러나 위반 사항은 메트릭에 보고됨). |
-- MAGIC 
-- MAGIC #### References to DLT Tables and Views
-- MAGIC 다른 DLT 테이블 및 뷰에 대한 참조에는 항상 **`live.`** prefix(접두사)가 포함됩니다. 대상 데이터베이스 이름은 런타임 시 자동으로 대체되므로 DEV/QA/PROD 환경 간에 파이프라인을 쉽게 마이그레이션할 수 있습니다.
-- MAGIC 
-- MAGIC #### References to Streaming Tables
-- MAGIC 
-- MAGIC 스트리밍 DLT 테이블에 대한 참조는 **`STREAM()`** 을 사용하여 테이블 이름을 인수로 제공합니다.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned sales orders with valid order_number(s)."
AS
  SELECT f.customer_id, f.customer_name, f.number_of_line_items, 
         timestamp(from_unixtime((cast(f.order_datetime as long)))) as order_datetime, 
         date(from_unixtime((cast(f.order_datetime as long)))) as order_date, 
         f.order_number, f.ordered_products, c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
  FROM STREAM(LIVE.sales_orders_raw) f
  LEFT JOIN LIVE.customers c
    ON c.customer_id = f.customer_id
    AND c.customer_name = f.customer_name

-- COMMAND ----------

-- MAGIC %md <i18n value="ae062501-5a39-4183-976a-53662619d516"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Declare Gold Table
-- MAGIC 
-- MAGIC 아키텍처의 가장 정제된 수준에서 우리는 비즈니스 가치가 있는 집계를 제공하는 테이블을 선언합니다. 이 경우에는 특정 지역을 기반으로 하는 판매 주문 데이터 모음입니다. 집계 시 보고서는 날짜 및 고객별로 주문 수와 합계를 생성합니다.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE sales_order_in_la
COMMENT "Sales orders in LA."
AS
  SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, 
         sum(ordered_products_explode.price) as sales, 
         sum(ordered_products_explode.qty) as quantity, 
         count(ordered_products_explode.id) as product_count
  FROM (SELECT city, order_date, customer_id, customer_name, explode(ordered_products) as ordered_products_explode
        FROM LIVE.sales_orders_cleaned 
        WHERE city = 'Los Angeles')
  GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

-- MAGIC %md <i18n value="c3efd15d-6587-4cd4-9758-37643dcfd694"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Explore Results
-- MAGIC 
-- MAGIC 파이프라인에 관련된 Entity와 Entity간의 관계를 나타내는 DAG(Directed Acyclic Graph)를 탐색합니다. 다음을 포함하는 요약을 보려면 각각을 클릭하십시오.
-- MAGIC * Run status
-- MAGIC * Metadata summary
-- MAGIC * Schema
-- MAGIC * Data quality metrics
-- MAGIC 
-- MAGIC Refer to this <a href="$./3. Pipeline Results" target="_blank">companion notebook</a> to inspect tables and logs.

-- COMMAND ----------

-- MAGIC %md <i18n value="77c262ce-1b24-4ca2-a931-e481927d1739"/>
-- MAGIC 
-- MAGIC 
-- MAGIC ## Update Pipeline
-- MAGIC 
-- MAGIC 다른 Gold 테이블을 선언하려면 다음 셀을 Uncomment 하십시오. 이전 골드 테이블 선언과 유사하게,이 Chicago의 **`city`** 에 대한 필터링을 적용하세요.
-- MAGIC 
-- MAGIC 파이프 라인을 다시 실행하여 업데이트 된 결과를 검사하십시오.
-- MAGIC 
-- MAGIC 예상대로 실행됩니까?
-- MAGIC 
-- MAGIC 문제를 식별 할 수 있습니까?

-- COMMAND ----------

-- TODO
-- CREATE OR REFRESH LIVE TABLE sales_order_in_chicago
-- COMMENT "Sales orders in Chicago."
-- AS
--   SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, 
--          sum(ordered_products_explode.price) as sales, 
--          sum(ordered_products_explode.qty) as quantity, 
--          count(ordered_products_explode.id) as product_count
--   FROM (SELECT city, order_date, customer_id, customer_name, explode(ordered_products) as ordered_products_explode
--         FROM sales_orders_cleaned 
--         WHERE city = 'Chicago')
--   GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
