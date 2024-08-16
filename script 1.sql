
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
  storage_aws_role_arn = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx'
  storage_allowed_locations = ('s3://xxxxxxxxxxxxxxxx/snowflake-data/RAW_DATA/');
 
 
desc integration my_s3_integration_external;
 
 
create or replace stage capston_stage
  storage_integration = my_s3_integration_external
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


REVOKE APPLYBUDGET ON DATABASE capston FROM ROLE PC_DBT_ROLE;
 
grant all privileges on DATABASE capston to role PC_DBT_ROLE;
 
grant all privileges on schema RAW_DATA to role PC_DBT_ROLE;
 
grant select on all tables in schema RAW_DATA to role PC_DBT_ROLE;

GRANT SELECT ON FUTURE TABLES IN DATABASE capston TO ROLE PC_DBT_ROLE;


ccreate or replace function trans_func(amount number)
returns string
language sql 
as
$$
select case when amount > 500 then 'High'
        when amount between 100 and 200 then 'Medium'
        else 'Low' end
 
$$
;   
 
grant USAGE on FUNCTION trans_func(NUMBER) to role PC_DBT_ROLE;
 
 
select *, trans_func(amount) as risk_level from transacation_raw;
 
 
CREATE MASKING POLICY CAPSTON.raw_data.EMAIL_MASK AS
(EMAIL VARCHAR) RETURNS VARCHAR ->
CASE WHEN CURRENT_ROLE = 'ADMIN' THEN EMAIL
ELSE REGEXP_REPLACE(EMAIL, '.+\@', '*****@')
END;
 
ALTER TABLE CAPSTON.raw_data.CUSTOMER_RAW MODIFY COLUMN email SET MASKING POLICY CAPSTON.raw_data.EMAIL_MASK;
 
 
CREATE MASKING POLICY CAPSTON.raw_data.Phone_MASK AS
(PHONE VARCHAR) RETURNS VARCHAR ->
CASE WHEN CURRENT_ROLE = 'ADMIN' THEN PHONE
ELSE SUBSTR(PHONE, 0, 5) || '***-****'
END;
 
 
ALTER TABLE CAPSTON.raw_data.CUSTOMER_RAW MODIFY COLUMN phone_number SET MASKING POLICY CAPSTON.raw_data.Phone_MASK;
 
 
CREATE OR REPLACE MASKING POLICY CAPSTON.raw_data.customer_id_MASK AS
(Cust_id VARCHAR) RETURNS VARCHAR ->
CASE
WHEN CURRENT_ROLE() = 'ADMIN' THEN Cust_id
ELSE 'XXXXXX'
END;
 
 
ALTER TABLE CAPSTON.raw_data.CUSTOMER_RAW MODIFY COLUMN phone_number SET MASKING POLICY CAPSTON.raw_data.customer_id_MASK;