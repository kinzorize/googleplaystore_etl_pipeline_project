import redshift_connector
import boto3
from io import BytesIO  # python3; python2:
import time
import pandas as pd
from io import StringIO
import io

AWS_ACCESS_KEY = ""  # Omitted my access key
AWS_SECRET_KEY = ""  # omitted my secret key for security reason
AWS_REGION = "us-east-1"
SCHEMA_NAME = "google_playstore_kingsley"
S3_STAGING_DIR = "s3://google-playstore-output/output/"
S3_BUCKET_NAME = "google-playstore-output"
S3_OUTPUT_DIRECTORY = "output"

# using boto3 to connect to aws athena
athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

# this function runs some query on athena and return the result to s3 bucket
Dict = {}


def download_and_load_query_results(
    client: boto3.client, query_response: Dict
) -> pd.DataFrame:
    while True:
        try:
            # This function only loads the first 1000 rows
            client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err
    temp_file_location: str = "athena_query_result.csv"
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,

    )
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
        temp_file_location,
    )
    return pd.read_csv(temp_file_location)


response = athena_client.start_query_execution(
    QueryString="SELECT * FROM googleplaystore",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    },
)
response
googleplaystore = download_and_load_query_results(athena_client, response)

googleplaystore.head()

response = athena_client.start_query_execution(
    QueryString="SELECT * FROM googleplaystore_review",
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
    },
)
googleplaystore_review = download_and_load_query_results(
    athena_client, response)

googleplaystore_review.head()

# grab the first row of the new_header
new_header = googleplaystore_review.iloc[0]
# take the data below the header row
googleplaystore_review = googleplaystore_review[1:]
# set the new header as the dataframe header
googleplaystore_review.columns = new_header

googleplaystore_review.head()

googleplaystore_clean = googleplaystore[['app', 'category', 'rating', 'reviews', 'size',
                                         'installs', 'price', 'content rating', 'genres', 'last updated', 'current ver', 'android ver']]

googleplaystore_clean.head()

bucket = 'google-paystore-etl'

# copy or load the transformed table to s3
csv_buffer = StringIO()
googleplaystore_clean.to_csv(csv_buffer)
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY,
                             region_name=AWS_REGION, )
s3_resource.Object(
    bucket, 'output/googleplaystore_clean.csv').put(Body=csv_buffer.getvalue())


csv_buffer = StringIO()
googleplaystore_clean.to_csv(csv_buffer)
s3_resource = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY,
                             region_name=AWS_REGION, )
s3_resource.Object(
    bucket, 'output/googleplaystore_review.csv').put(Body=csv_buffer.getvalue())

googleplaystore_clean_sql = pd.io.sql.get_schema(
    googleplaystore_clean.reset_index(), 'googleplaystore_clean')
print(''.join(googleplaystore_clean_sql))

googleplaystore_review_sql = pd.io.sql.get_schema(
    googleplaystore_review.reset_index(), 'googleplaystore_review')
print(''.join(googleplaystore_review_sql))


conn = redshift_connector.connect(
    host='redshift-cluster-1.cq86xufekc54.us-east-1.redshift.amazonaws.com',
    database='dev',
    user='awsuser',
    password='123Success'
)

conn.autocommit = True

cursor = redshift_connector.Cursor = conn.cursor()

cursor.execute("""CREATE TABLE "googleplaystore_clean" (
"index" INTEGER,
  "app" TEXT,
  "category" TEXT,
  "rating" TEXT,
  "reviews" REAL,
  "size" TEXT,
  "installs" TEXT,
  "price" TEXT,
  "content rating" TEXT,
  "genres" TEXT,
  "last updated" TEXT,
  "current ver" TEXT,
  "android ver" TEXT
)
""")

cursor.execute("""
CREATE TABLE "googleplaystore_review" (
"index" INTEGER,
  "App" TEXT,
  "Translated_Review" TEXT,
  "Sentiment" TEXT,
  "Sentiment_Polarity" TEXT,
  "Sentiment_Subjectivity" TEXT
)
""")
cursor.execute("""
copy googleplaystore_clean from 's3://elijah-covid-project/output/googleplaystore_clean.csv'
credentials 'aws_iam_role:arn:aws:iam::490101006133:role/redshift-s3-access'
delimiter ','
region 'us-east-1'
IGNOREHEADER 1

""")

cursor.execute("""
copy googleplaystore_review from 's3://elijah-covid-project/output/googleplaystore_review.csv'
credentials 'aws_iam_role:arn:aws:iam::490101006133:role/redshift-s3-access'
delimiter ','
region 'us-east-1'
IGNOREHEADER 1

""")
