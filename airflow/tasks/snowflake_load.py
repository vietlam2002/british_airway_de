from snowflake import connector
import pathlib
from dotenv import dotenv_values

# Load config
script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

conn = connector.connect(
    user=config.get("snowflake_user"),
    password=config.get("snowflake_password"),
    account=config.get("snowflake_account")
)

access_key = config.get("aws_access_key_id")
secret_key = config.get("aws_secret_access_key")

cur = conn.cursor()

# Tạo database/schema
cur.execute("CREATE DATABASE IF NOT EXISTS british_db;")
cur.execute("CREATE SCHEMA IF NOT EXISTS british_db.RAW;")
cur.execute("CREATE SCHEMA IF NOT EXISTS british_db.MODEL;")

# Tạo bảng (bạn có thể thêm phần CREATE TABLE nếu chưa tạo)

# Load dữ liệu từ S3
cur.execute(f"""
COPY INTO british_db.RAW.BRITISH_DATA
FROM 's3://datalakechat/clean_data.csv'
CREDENTIALS = (
    AWS_KEY_ID='{access_key}',
    AWS_SECRET_KEY='{secret_key}'
)
FILE_FORMAT = (
    TYPE=CSV,
    FIELD_DELIMITER=',',
    SKIP_HEADER=1,
    FIELD_OPTIONALLY_ENCLOSED_BY='"',
    ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
)
ON_ERROR = 'CONTINUE';
""")
