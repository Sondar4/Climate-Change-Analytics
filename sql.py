import configparser

# --- CONFIG ---

config = configparser.ConfigParser()
config.read('dwh.cfg')

# --- DROP TABLES ---

staging_emissions_drop = 'DROP TABLE IF EXISTS staging_emissions;'
staging_temperatures_drop = 'DROP TABLE IF EXISTS staging_temperatures;'
staging_industrial_production_drop = 'DROP TABLE IF EXISTS staging_industrial_production;'
staging_currencies_drop = 'DROP TABLE IF EXISTS staging_currencies;'

industrial_production_drop = 'DROP TABLE IF EXISTS industrial_production;'
countries_drop = 'DROP TABLE IF EXISTS countries;'
temperatures_drop = 'DROP TABLE IF EXISTS temperatures;'
currencies_drop = 'DROP TABLE IF EXISTS currencies;'
industries_drop = 'DROP TABLE IF EXISTS industries;'

# --- CREATE TABLES ---

staging_emissions_create = """CREATE TABLE IF NOT EXISTS staging_emissions(
    country text,
    country_code text,
    year smallint,
    emissions double precision
);"""

staging_temperatures_create = """CREATE TABLE IF NOT EXISTS staging_temperatures(
    dt date,
    avg_temp double precision,
    avg_temp_uncertainty double precision,
    country text
);"""

staging_industrial_production_create = """CREATE TABLE IF NOT EXISTS staging_industrial_production(
    country_code text,
    country text,
    value_type_code text,
    value_type text,
    industry_id text,
    industry text,
    YEAR_1 smallint,
    year smallint,
    currency_code text,
    currency text,
    power_code_code int,
    power_code text,
    reference_period_code text,
    reference_period text,
    value numeric(30, 10),
    flag_codes text,
    flags VARCHAR(350)
);"""

staging_currencies_create = """CREATE TABLE IF NOT EXISTS staging_currencies(
    dt date,
    currency_code text,
    value numeric(20, 10)
);"""

industrial_production_create = """CREATE TABLE IF NOT EXISTS industrial_production(
    country_code VARCHAR(3),
    industry_id VARCHAR(15) sortkey,
    year smallint,
    currency_code VARCHAR(3),
    value numeric(30, 10),
    PRIMARY KEY (country_code, industry_id, year)
);"""

countries_create = """CREATE TABLE IF NOT EXISTS countries(
    country_code VARCHAR(3),
    year smallint,
    emissions double precision,
    PRIMARY KEY (country_code, year)
);"""

temperatures_create = """CREATE TABLE IF NOT EXISTS temperatures(
    country_code VARCHAR(3) distkey,
    dt date sortkey,
    year smallint,
    average_temp double precision,
    average_temp_uncertainty double precision,
    PRIMARY KEY (country_code, dt)
);"""

currencies_create = """CREATE TABLE IF NOT EXISTS currencies(
    dt date sortkey,
    year smallint,
    currency_code VARCHAR(3) distkey,
    to_dollars numeric(20, 10),
    PRIMARY KEY (dt, currency_code)
);"""

industries_create = """CREATE TABLE IF NOT EXISTS industries(
    industry_id VARCHAR(15) PRIMARY KEY,
    description text
);"""

# --- STAGING TABLES ---

staging_emissions_copy = """
    copy staging_emissions from '{}'
    credentials 'aws_iam_role={}'
    region '{}'
    delimiter ','
    ignoreheader 1
;""".format(config.get('S3', 'EMISSIONS_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'REGION'))

staging_temperatures_copy = """
    copy staging_temperatures from '{}'
    credentials 'aws_iam_role={}'
    region '{}'
    delimiter ','
    ignoreheader 1
    removequotes
;""".format(config.get('S3', 'TEMPERATURES_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'REGION'))

staging_industrial_production_copy = """
    copy staging_industrial_production from '{}'
    credentials 'aws_iam_role={}'
    region '{}'
    delimiter ','
    ignoreheader 1
    removequotes
;""".format(config.get('S3', 'INDUSTRIAL_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'REGION'))

staging_currencies_copy = """
    copy staging_currencies from '{}'
    credentials 'aws_iam_role={}'
    region '{}'
    delimiter ','
    ignoreheader 1
    NULL AS 'ND'
;""".format(config.get('S3', 'CURRENCIES_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'REGION'))


# --- FINAL TABLES ---

industrial_production_insert = """
INSERT INTO industrial_production (country_code, industry_id, year, currency_code, value)
    SELECT country_code, industry_id, year, currency_code, value * POWER(10, power_code_code)
    FROM staging_industrial_production
    WHERE value_type_code = 'VALU'
;"""

countries_insert = """
INSERT INTO countries (country_code, year, emissions)
    SELECT country_code, year, emissions
    FROM staging_emissions
    WHERE country_code IS NOT NULL
        AND country_code != 'OWID_WRL'
        AND country_code != 'OWID_CZS'
        AND country_code != ' '
;"""

temperatures_insert = """
INSERT INTO temperatures (country_code, dt, year, average_temp, average_temp_uncertainty) 
WITH countries AS (
	SELECT DISTINCT country, country_code 
    FROM staging_emissions
    WHERE country_code IS NOT NULL
        AND country_code != 'OWID_WRL'
        AND country_code != 'OWID_CZS'
        AND country_code != ' '
)
SELECT c.country_code, t.dt, EXTRACT(y from t.dt), t.avg_temp, t.avg_temp_uncertainty
    FROM staging_temperatures t
    LEFT JOIN countries c
        ON c.country = t.country
    WHERE country_code IS NOT NULL
        AND t.dt IS NOT NULL
;"""

currencies_insert = """
INSERT INTO currencies (dt, year, currency_code, to_dollars)
    SELECT dt, EXTRACT(y from dt), currency_code, value
    FROM staging_currencies
        WHERE dt IS NOT NULL
            AND value IS NOT NULL
;"""

industries_insert = """
INSERT INTO industries (industry_id, description)
    SELECT DISTINCT industry_id, industry
    FROM staging_industrial_production
    WHERE value_type_code = 'VALU'
;"""

# --- QUERY LISTS ---
 
drop_table_queries = [
    staging_emissions_drop,
    staging_temperatures_drop,
    staging_industrial_production_drop,
    staging_currencies_drop,
    industrial_production_drop,
    countries_drop,
    temperatures_drop,
    currencies_drop,
    industries_drop
]

create_table_queries = [
    staging_emissions_create,
    staging_temperatures_create,
    staging_industrial_production_create,
    staging_currencies_create,
    industrial_production_create,
    countries_create,
    temperatures_create,
    currencies_create,
    industries_create
]

copy_table_queries = [
    staging_emissions_copy,
    staging_temperatures_copy,
    staging_industrial_production_copy,
    staging_currencies_copy
]

insert_table_queries  = [
    industrial_production_insert,
    countries_insert,
    temperatures_insert,
    currencies_insert,
    industries_insert
]