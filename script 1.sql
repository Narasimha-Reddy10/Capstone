
CREATE OR REPLACE DATABASE CAPSTON;

CREATE OR REPLACE SCHEMA  CAPSTON.raw_data;
CREATE OR REPLACE SCHEMA  CAPSTON.transformed_data;
CREATE OR REPLACE SCHEMA CAPSTON.analytics;
CREATE OR REPLACE SCHEMA CAPSTON.security;


CREATE OR REPLACE TABLE CAPSTON.raw_data.transactions(
    transaction_id STRING,
    customer_id STRING,
    transaction_date TIMESTAMP_NTZ,
    amount FLOAT,
    currency STRING,
    transaction_type STRING,
    channel STRING,
    merchant_name STRING,
    merchant_category STRING,
    location_country STRING,
    location_city STRING,
    is_flagged BOOLEAN
    );
    

CREATE OR REPLACE TABLE CAPSTON.raw_data.customers(
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    city STRING,
    country STRING,
    occupation STRING,
    income_bracket STRING,
    customer_since DATE
);

-- Create table for raw account data
CREATE OR REPLACE TABLE CAPSTON.raw_data.accounts(
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    account_status STRING,
    open_date DATE,
    current_balance FLOAT,
    currency STRING,
    credit_limit FLOAT
);

-- Create table for raw credit bureau data
CREATE OR REPLACE TABLE CAPSTON.raw_data.credit_bureau (
    customer_id STRING,
    credit_score INT,
    number_of_credit_accounts INT,
    total_credit_limit FLOAT,
    total_credit_used FLOAT,
    number_of_late_payments INT,
    bankruptcies INT
);

-- Create table for raw watchlist data
CREATE OR REPLACE TABLE CAPSTON.raw_data.watchlist(
    entity_id STRING,
    entity_name STRING,
    entity_type STRING,
    risk_category STRING,
    listed_date DATE,
    source STRING
);



-- creating table for transformation



CREATE OR REPLACE TABLE CAPSTON.TRANSFORMED_DATA.transactions(
    transaction_id STRING,
    customer_id STRING,
    transaction_date TIMESTAMP_NTZ,
    amount FLOAT,
    currency STRING,
    transaction_type STRING,
    channel STRING,
    merchant_name STRING,
    merchant_category STRING,
    location_country STRING,
    location_city STRING,
    is_flagged BOOLEAN
    );
    

CREATE OR REPLACE TABLE CAPSTON.TRANSFORMED_DATA.customers(
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    city STRING,
    country STRING,
    occupation STRING,
    income_bracket STRING,
    customer_since DATE
);

-- Create table for raw account data
CREATE OR REPLACE TABLE CAPSTON.TRANSFORMED_DATA.accounts(
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    account_status STRING,
    open_date DATE,
    current_balance FLOAT,
    currency STRING,
    credit_limit FLOAT
);

-- Create table for raw credit bureau data
CREATE OR REPLACE TABLE CAPSTON.TRANSFORMED_DATA.credit_bureau (
    customer_id STRING,
    credit_score INT,
    number_of_credit_accounts INT,
    total_credit_limit FLOAT,
    total_credit_used FLOAT,
    number_of_late_payments INT,
    bankruptcies INT
);

-- Create table for raw watchlist data
CREATE OR REPLACE TABLE CAPSTON.TRANSFORMED_DATA.watchlist(
    entity_id STRING,
    entity_name STRING,
    entity_type STRING,
    risk_category STRING,
    listed_date DATE,
    source STRING
);


-- creating table for analysis



CREATE OR REPLACE TABLE CAPSTON.ANALYTICS.transactions(
    transaction_id STRING,
    customer_id STRING,
    transaction_date TIMESTAMP_NTZ,
    amount FLOAT,
    currency STRING,
    transaction_type STRING,
    channel STRING,
    merchant_name STRING,
    merchant_category STRING,
    location_country STRING,
    location_city STRING,
    is_flagged BOOLEAN
    );
    

CREATE OR REPLACE TABLE CAPSTON.ANALYTICS.customers(
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    city STRING,
    country STRING,
    occupation STRING,
    income_bracket STRING,
    customer_since DATE
);

-- Create table for raw account data
CREATE OR REPLACE TABLE CAPSTON.ANALYTICS.accounts(
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    account_status STRING,
    open_date DATE,
    current_balance FLOAT,
    currency STRING,
    credit_limit FLOAT
);

-- Create table for raw credit bureau data
CREATE OR REPLACE TABLE CAPSTON.ANALYTICS.credit_bureau (
    customer_id STRING,
    credit_score INT,
    number_of_credit_accounts INT,
    total_credit_limit FLOAT,
    total_credit_used FLOAT,
    number_of_late_payments INT,
    bankruptcies INT
);

-- Create table for raw watchlist data
CREATE OR REPLACE TABLE CAPSTON.ANALYTICS.watchlist(
    entity_id STRING,
    entity_name STRING,
    entity_type STRING,
    risk_category STRING,
    listed_date DATE,
    source STRING
);


-----------------------------------------------


create or replace file format MY_csv_FORMAT
type = 'csv';



create or replace STORAGE INTEGRATION my_s3_integration_external
  type = External_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::905418194718:role/Capstone'
  storage_allowed_locations = ('s3://snowflake-data-sneha1/snowflake-data/RAW_DATA/');
 
 
desc integration my_s3_integration_external;
 
 
create or replace stage capston_stage
  storage_integration = my_s3_integration_external
  url = 's3://snowflake-data-sneha1/snowflake-data/RAW_DATA/'
;

list @CAPSTON.RAW_DATA.CAPSTON_STAGE;

-- Create a Snowpipe with Auto Ingest Enabled

CREATE OR REPLACE PIPE rawdata_accounts_pipe
AUTO_INGEST = TRUE AS
COPY INTO accounts
FROM @CAPSTON_STAGE
ON_ERROR = CONTINUE;

CREATE OR REPLACE PIPE rawdata_CREDIT_BUREAU
AUTO_INGEST = TRUE AS
COPY INTO CAPSTON.RAW_DATA.CREDIT_BUREAU
FROM @CAPSTON_STAGE
ON_ERROR = CONTINUE;

CREATE OR REPLACE PIPE rawdata_CUSTOMERS
AUTO_INGEST = TRUE AS
COPY INTO CAPSTON.RAW_DATA.CUSTOMERS
FROM @CAPSTON_STAGE
ON_ERROR = CONTINUE;


CREATE OR REPLACE PIPE rawdata_Transactions
AUTO_INGEST = TRUE AS
COPY INTO CAPSTON.RAW_DATA.TRANSACTIONS
FROM @CAPSTON_STAGE
ON_ERROR = CONTINUE;

alter pipe rawdata_accounts_pipe refresh;
alter pipe rawdata_CREDIT_BUREAU refresh;
alter pipe rawdata_CUSTOMERS refresh;
alter pipe rawdata_Transactions refresh;


list @CAPSTON_STAGE;

-- lISTING
SHOW PIPES;

select * from CAPSTON.RAW_DATA.accounts;
select * from capston.raw_data.credit_bureau;
select * from capston.raw_data.customers;
select * from capston.raw_data.transactions;


-- stream


REVOKE APPLYBUDGET ON DATABASE capston FROM ROLE PC_DBT_ROLE;
 
grant all privileges on DATABASE capston to role PC_DBT_ROLE;
 
grant all privileges on schema RAW_DATA to role PC_DBT_ROLE;
 
grant select on all tables in schema RAW_DATA to role PC_DBT_ROLE;

GRANT SELECT ON FUTURE TABLES IN DATABASE capston TO ROLE PC_DBT_ROLE;


create or replace function trans_func(amount number)
returns string
language sql 
as
$$
select case when amount > 5000 then 'High'
        when amount between 2000 and 1000 then 'Medium'
        else 'Low' end
 
$$
;   
 
grant USAGE on FUNCTION trans_func(NUMBER) to role PC_DBT_ROLE;
 
 
select *, trans_func(amount) as risk_level from CAPSTON.RAW_DATA.TRANSACTIONS;


CREATE OR REPLACE FUNCTION age_fun(age NUMBER)
RETURNS STRING
LANGUAGE SQL
AS
$$
SELECT CASE
        WHEN age = 0 THEN '18'
        WHEN age IS NULL THEN 'Unknown'
        ELSE TO_CHAR(age)
       END
$$;
  
 
GRANT USAGE ON FUNCTION age_fun(NUMBER) TO ROLE PC_DBT_ROLE;
 
 
select *, age_fun(amount) as age_correction from CAPSTON.RAW_DATA.TRANSACTIONS;



create or replace stream CAPSTON.RAW_DATA.ACCOUNTS_stream on table accounts;

create or replace stream CAPSTON.RAW_DATA.CREDIT_BUREAU_stream on table CREDIT_BUREAU;

create or replace stream CAPSTON.RAW_DATA.CUSTOMERS_stream on table CUSTOMERS;

create or replace stream CAPSTON.RAW_DATA.TRANSACTIONS_stream on table TRANSACTIONS;






SELECT * FROM ACCOUNTS_stream;
SELECT * FROM CREDIT_BUREAU_stream ;

SELECT * FROM CUSTOMERS_stream;

SELECT * FROM TRANSACTIONS_stream;


-- to see the arn on notification
desc pipe TRANSACTIONS_stream;


CREATE OR REPLACE TASK CAPSTON.RAW_DATA.ACCOUNTS_task
WAREHOUSE='COMPUTE_WH'
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('CAPSTON.RAW_DATA.ACCOUNTS_stream') 
AS
INSERT INTO CAPSTON.RAW_DATA.transactions (            TRANSACTION_ID,CUSTOMER_ID,TRANSACTION_DATE,AMOUNT,CURRENCY,TRANSACTION_TYPE,CHANNEL,
MERCHANT_NAME,MERCHANT_CATEGORY,LOCATION_COUNTRY,LOCATION_CITY,IS_FLAGGED
        )
        SELECT rt.* FROM CAPSTON.RAW_DATA.transactions rt 
        JOIN CAPSTON.RAW_DATA.transactions_stream s1 
        ON s1.CUSTOMER_ID = rt.CUSTOMER_ID;

        

CREATE OR REPLACE TASK CAPSTON.RAW_DATA.CREDIT_BUREAU_task
WAREHOUSE='COMPUTE_WH'
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('CAPSTON.RAW_DATA.CREDIT_BUREAU_stream') AS
CALL CAPSTON.RAW_DATA.upload_account_data()
;

CREATE OR REPLACE TASK CAPSTON.RAW_DATA.CREDIT_BUREAU_task
WAREHOUSE='COMPUTE_WH'
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('CAPSTON.RAW_DATA.CREDIT_BUREAU_stream') AS
CALL CAPSTON.RAW_DATA.upload_account_data()
;



