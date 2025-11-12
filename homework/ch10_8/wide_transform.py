from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, count
import time

spark = SparkSession \
    .builder \
    .appName("wide_transform") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.instances", "3") \
    .getOrCreate()

print("=== DataFrame app Start ===")


jobs_df = "hdfs:///home/spark/sample/linkedin_jobs/jobs/job_skills.csv"
jobs_schema = 'job_id LONG, skill_abr STRING'
skills_df = "hdfs:///home/spark/sample/linkedin_jobs/mappings/skills.csv"
skills_schema = 'skill_abr STRING, skill_name STRING'

skills_df = spark.read \
    .option("header", "true") \
    .option('multiLine', 'true') \
    .schema(skills_schema) \
    .csv(skills_df)

jobs_df = spark.read \
    .option("header", "true") \
    .option('multiLine', 'true') \
    .schema(jobs_schema) \
    .csv(jobs_df)

# 조인 수행 전 확인
jobs_df.persist()
jobs_df.show()

# # 조인 수행, skills DataFrame을 broadcast 처리
# join_df = jobs_df.join(
#     other = broadcast(skills_df),
#     on = 'skill_abr')

# # 최종 데이터프레임 컬럼 ['skill_name','job_count'] 기준 내림차순
# result_df = join_df.groupBy('skill_name') \
#     .count() \
#     .withColumnRenamed('count', 'job_count') \
#     .sort(col('job_count').desc())

# 조인 수행, jobs DataFrame을 broadcast 처리
join_df = jobs_df.join(
    other = broadcast(skills_df),
    on = 'skill_abr',
    how = 'inner'
).select('job_id', 'skill_name') \
    .groupBy('skill_name') \
    .agg(count('job_id').alias('job_count')) \
    .sort('job_count', ascending=False)

join_df.show(20)

time.sleep(1200)
