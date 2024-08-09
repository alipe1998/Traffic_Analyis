from pathlib import Path
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Append the current working directory to the system path to import the updated script
sys.path.append(str(Path.cwd().parent))

# Import the function from the updated script
from src.spark_clustering import load_and_process_crash_data
import config
# Configuration for AWS access keys (recommended to use environment variables)
AWS_ACCESS_KEY_ID = config.aws_access_key_id
AWS_SECRET_ACCESS_KEY = config.aws_secret_access_key

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import config

# Initialize Spark session with AWS credentials
conf = SparkConf() \
    .setAppName("CrashDataProcessor") \
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .set("spark.hadoop.fs.s3a.access.key", config.aws_access_key_id) \
    .set("spark.hadoop.fs.s3a.secret.key", config.aws_secret_access_key) \
    .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")

sc = SparkContext(conf=conf)
spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

# Set logging level to reduce verbosity
spark.sparkContext.setLogLevel("ERROR")

################################## run batch file on data ##################################

def run():
    S3_OUTPUT_PATH = 's3a://public-crash-data/clean-data/'
    
    # S3 URL for the input data
    s3_url = 's3a://public-crash-data/raw-data/combined_cleaned_group_crash.csv'
    
    # Load and process crash data
    crash_data_object = load_and_process_crash_data(spark, s3_url)
    
    # Assemble features for clustering
    crash_data_object.assemble_features()
    
    # Display the DataFrame schema and some data
    crash_data_object.regions_df.show()
    crash_data_object.regions_df.printSchema()
    
    # Run KMeans clustering
    crash_data_clustered = crash_data_object.KMeans_model()
    
    # Compute fatality rate and save results to S3
    crash_data_clustered.compute_fatality_rate(
        cluster_col='kmeans_cluster',
        save_to_s3=True,
        s3_path=S3_OUTPUT_PATH
    )

run()