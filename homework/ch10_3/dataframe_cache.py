from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from time import sleep

spark = SparkSession.builder.appName("Homework_ch10_3").getOrCreate()

# 파일 경로 정의
path_industries = "hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv"
path_employees = "hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv"

# 스키마 정의
schema_industries = "company_id STRING, name STRING, description STRING, company_size INT, state STRING, country STRING, city STRING, zip_code STRING, address STRING, url STRING, industry STRING"
schema_employees = "company_id STRING, employee_count INT"

# 회사별 산업 도메인 정보 로드
industries_df = spark.read \
    .option("header", "true") \
    .option("multiLine", "true") \
    .schema(schema_industries) \
    .csv(path_industries)

industries_df.persist()  # 메모리에 캐싱
print("Industries count:", industries_df.count())

# 회사별 종업원 수 정보 로드
employees_df = spark.read \
    .option("header", "true") \
    .option("multiLine", "true") \
    .schema(schema_employees) \
    .csv(path_employees)

employees_df.persist()
print("Employees count:", employees_df.count())
