from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from time import sleep

spark = SparkSession.builder.appName("dataframe_cache").getOrCreate()

print("=== DataFrame app Start ===")

company_emp_path = "hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv"
company_emp_schema = "company_id LONG, employee_count LONG, follower_count LONG, time_recorded TIMESTAMP"
company_ind_path = "hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv"
company_ind_schema = "company_id LONG, industry STRING"

# employees counts Load
company_emp_df = spark.read \
    .option("header", "true") \
    .schema(company_emp_schema) \
    .csv(company_emp_path)
company_emp_df.persist()
emp_cnt = company_emp_df.count()
print(f"Employee counts: {emp_cnt}")


# # SparkSession 생성
# spark = SparkSession.builder.appName("Homework_ch10_3").getOrCreate()

# # 파일 경로 정의
# path_industries = "hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv"
# path_employees = "hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv"

# # 스키마 정의
# schema_industries = "company_id STRING, name STRING, description STRING, company_size INT, state STRING, country STRING, city STRING, zip_code STRING, address STRING, url STRING, industry STRING"
# schema_employees = "company_id STRING, employee_count INT"

# # 회사별 산업 도메인 정보 로드
# industries_df = spark.read \
#     .option("header", "true") \
#     .option("multiLine", "true") \
#     .schema(schema_industries) \
#     .csv(path_industries)

# industries_df.persist()
# print("Industries count:", industries_df.count())

# # 회사별 종업원 수 정보 로드
# employees_df = spark.read \
#     .option("header", "true") \
#     .option("multiLine", "true") \
#     .schema(schema_employees) \
#     .csv(path_employees)

# employees_df.persist()
# print("Employees count:", employees_df.count())

# # 테스트용으로 5행씩 출력
# print("=== Sample from industries_df ===")
# industries_df.show(5)

# print("=== Sample from employees_df ===")
# employees_df.show(5)

# # 프로그램 유지 (unpersist 하지 말고)
# sleep(300)
