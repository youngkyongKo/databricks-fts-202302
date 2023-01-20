# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md <i18n value="1fb32f72-2ccc-4206-98d9-907287fc3262"/>
# MAGIC 
# MAGIC 
# MAGIC # 델타 라이브 테이블 UI 사용
# MAGIC 
# MAGIC 이 데모에서는 DLT UI를 살펴봅니다.
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC 이 단원을 마치면 다음을 수행할 수 있습니다.
# MAGIC * DLT 파이프라인 배포
# MAGIC * 결과 DAG 탐색
# MAGIC * 파이프라인 업데이트 실행
# MAGIC * 지표 살펴보기

# COMMAND ----------

# MAGIC %md <i18n value="c950ed75-9a93-4340-a82c-e00505222d15"/>
# MAGIC 
# MAGIC 
# MAGIC ## Run Setup
# MAGIC 
# MAGIC 다음 셀은 이 데모를 재설정하도록 구성됩니다.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-08.1.1

# COMMAND ----------

# MAGIC %md <i18n value="0a719ade-b4b5-49b5-89bf-8fc2b0b7d63c"/>
# MAGIC 
# MAGIC 
# MAGIC Execute the following cell to print out values that will be used during the following configuration steps.

# COMMAND ----------

DA.print_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="71b010a3-80be-4909-9b44-6f68029f16c0"/>
# MAGIC 
# MAGIC 
# MAGIC ## 파이프라인 생성 및 구성
# MAGIC 이 섹션에서는 함께 제공된 노트북을 사용하여 파이프라인을 생성합니다. 다음 레슨에서 노트북의 내용을 살펴보겠습니다.
# MAGIC 
# MAGIC 1. 사이드바에서 **Workflows** 버튼을 클릭합니다.
# MAGIC 1. **Delta Live Tables** 탭을 선택합니다.
# MAGIC 1. **Create Pipeline** 클릭합니다..
# MAGIC 1. **Product Edition** 을 **Advanced** 로 두십시오.
# MAGIC 1. **Pipeline Name** 이름 입력 - 이러한 이름은 고유해야 하므로 위 셀에 제공된 **Pipeline Name** 이름을 사용하는 것이 좋습니다.
# MAGIC 1. **Notebook Libraries** 의 경우, 탐색기를 사용하여 위에 지정된 노트북을 찾아 선택합니다.
# MAGIC    * 이 문서는 표준 Databricks Notebook이지만 SQL 구문은 DLT 테이블 선언에 특화되어 있습니다.
# MAGIC    * 다음 Excercise 에서 구문을 살펴보겠습니다.
# MAGIC 1. **Configuration** 에서 두 개의 구성 매개변수를 추가합니다:
# MAGIC    * **Add configuration** 클릭하고, "key" 를 **spark.master** 로 설정하고 "value" 에 **local[\*]** 로 설정합니다.
# MAGIC    * **Add configuration** 클릭하고, "key" 를 **datasets_path** 로 설정하고 "value" 에 위 셀에서 제공된 값으로 설정합니다.
# MAGIC 1. **Target** 필드에는 위 셀에서 제공된 데이터베이스 이름을 입력합니다.
# MAGIC **`<name>_<hash>_dbacademy_dewd_dlt_demo_81`** 패턴을 따라야 합니다
# MAGIC    * 이 필드는 선택 사항입니다. 지정하지 않으면 테이블이 메타스토어에 등록되지 않지만 DBFS에서 계속 사용할 수 있습니다. 이 옵션에 대한 자세한 내용은 설명서를 참조하십시오. <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#publish-tables" target="_blank">documentation</a>.   
# MAGIC 1. **Storage location** 필드에 위 셀에 제공된 경로를 입력합니다.
# MAGIC    * 이 선택적 필드를 통해 사용자는 파이프라인 실행과 관련된 로그, 테이블 및 기타 정보를 저장할 위치를 지정할 수 있습니다.. 
# MAGIC    * 지정하지 않으면 DLT가 자동으로 디렉토리를 생성합니다.
# MAGIC 1. **Pipeline Mode** 에서 **Triggered** 방법을 선택합니다.
# MAGIC    * 이 필드는 파이프라인 실행 방법을 지정합니다.
# MAGIC    * **Triggered** 파이프라인은 한 번 실행된 후에 종료가 되며 매뉴얼 또는 스케쥴 업데이트로 다시 수행할 수 있습니다.
# MAGIC    * **Continuous** 파이프라인은 지속적으로 실행되어 새로운 데이터가 도착하면 수집합니다. Latency 및 cost 요구 사항에 따라 모드를 선택합니다.
# MAGIC 1. **Enable autoscaling** 체크박스를 선택 취소합니다.
# MAGIC 1. **`workers`** 수를 **`0`** (zero) 으로 설정합니다.
# MAGIC    * 위의 **spark.master** 설정을 따라 **Single Node** clusters 가 생성됩니다.
# MAGIC 1. **Use Photon Acceleration** 체크박스를 체크 합니다.
# MAGIC 1. **Channel** 에서 **Current** 를 선택합니다.
# MAGIC 1. **Policy** 에 위 셀에 제공된 값을 선택합니다.
# MAGIC 
# MAGIC **Enable autoscaling**, **Min Workers**, **Max Workers** 필드는 파이프라인을 처리하는 기본 클러스터에 대한 작업자 구성을 제어합니다. 
# MAGIC 
# MAGIC 대화식 클러스터를 구성할 때 제공되는 것과 유사한 DBU 추정치가 제공됩니다.
# MAGIC 
# MAGIC 마지막으로 **Create** 클릭합니다.

# COMMAND ----------

DA.validate_pipeline_config()

# COMMAND ----------

# MAGIC %md <i18n value="a7e4b2fc-83a1-4509-8269-9a4c5791de21"/>
# MAGIC 
# MAGIC 
# MAGIC ## Run a Pipeline
# MAGIC 
# MAGIC With a pipeline created, you will now run the pipeline.
# MAGIC 
# MAGIC 1. Select **Development** to run the pipeline in development mode. 
# MAGIC   * Development mode provides for more expeditious iterative development by reusing the cluster (as opposed to creating a new cluster for each run) and disabling retries so that you can readily identify and fix errors.
# MAGIC   * Refer to the <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#optimize-execution" target="_blank">documentation</a> for more information on this feature.
# MAGIC 2. Click **Start**.
# MAGIC 
# MAGIC The initial run will take several minutes while a cluster is provisioned. 
# MAGIC 
# MAGIC Subsequent runs will be appreciably quicker.

# COMMAND ----------

# MAGIC %md <i18n value="4b92f93e-7a7f-4169-a1d2-9df3ac440674"/>
# MAGIC 
# MAGIC 
# MAGIC ## Exploring the DAG
# MAGIC 
# MAGIC As the pipeline completes, the execution flow is graphed. 
# MAGIC 
# MAGIC Selecting the tables reviews the details.
# MAGIC 
# MAGIC Select **sales_orders_cleaned**. Notice the results reported in the **Data Quality** section. Because this flow has data expectations declared, those metrics are tracked here. No records are dropped because the constraint is declared in a way that allows violating records to be included in the output. This will be covered in more details in the next exercise.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
