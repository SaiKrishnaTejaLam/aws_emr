import boto3
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

secrets_client = boto3.client('secretsmanager', region_name="us-east-2")
def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        
        if 'SecretString' in response:
            return json.loads(response['SecretString'])
        else:
            return json.loads(response['SecretBinary'])
    except Exception as e:
        print(f"Error retrieving secret {secret_name}: {str(e)}")
        return None

pg_credentials = get_secret("pgadmin_credentials")

if pg_credentials is None:
    print("Failed to fetch PostgreSQL credentials. Exiting the program.")
    sys.exit(1)

POSTGRES_HOST = pg_credentials['POSTGRES_HOST']
POSTGRES_USER = pg_credentials['POSTGRES_USER']
POSTGRES_PASSWORD = pg_credentials['POSTGRES_PASSWORD']
POSTGRES_DB = pg_credentials['POSTGRES_DB']
POSTGRES_PORT = int(pg_credentials['POSTGRES_PORT'])

BUCKET_NAME = "full-load-bucket"  

spark = SparkSession.builder.appName("PostgresToS3Transfer").getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "<AWS_ACCESS_KEY>") 
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "<AWS_SECRET_KEY>") 
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

try:
    print("Reading data from PostgreSQL...")
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "public.mockrecord") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    print("Data read complete, processing data...")
    df_cleaned = df.withColumn("questions_list", concat_ws(",", col("questions_list")))

    print("Writing data to S3...")
    df_cleaned.write.option("header", True) \
        .mode("overwrite") \
        .csv(f"s3://{BUCKET_NAME}/rawlayer/stepfunction")

    print("Data transfer completed successfully!")

except Exception as e:
    print(f"An error occurred: {e}", file=sys.stderr)

finally:
    print("Stopping Spark session...")
    spark.stop()    
