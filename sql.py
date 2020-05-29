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
    YEAR int,
    year int,
    currency_code text,
    currency text,
    power_code_code int,
    power_code text,
    reference_period_code text,
    reference_period text,
    value numeric(30, 10),
    flag_codes text,
    flags text
);"""

staging_currencies_create = """CREATE TABLE IF NOT EXISTS staging_currencies(
    id int,
    dt date,
    AUD numeric(20, 10),
    EUR numeric(20, 10),
    NZD numeric(20, 10),
    GBP numeric(20, 10),
    BRL numeric(20, 10),
    CAD numeric(20, 10),
    CNY numeric(20, 10),
    HKD numeric(20, 10),
    INR numeric(20, 10),
    KRW numeric(20, 10),
    MXN numeric(20, 10),
    ZAR numeric(20, 10),
    SGD numeric(20, 10),
    DKK numeric(20, 10),
    JPY numeric(20, 10),
    MYR numeric(20, 10),
    NOK numeric(20, 10),
    SEK numeric(20, 10),
    LKR numeric(20, 10),
    CHF numeric(20, 10),
    TWD numeric(20, 10),
    THB numeric(20, 10)
);"""

industrial_production_create = """CREATE TABLE IF NOT EXISTS industrial_production(
    country_code text,
    industry_id text,
    year int,
    currency_code text,
    value numeric(30, 10),

);"""

countries_create = """CREATE TABLE IF NOT EXISTS countries(
    country_code text,
    year int,
    emissions double precision
);"""

temperatures_create = """CREATE TABLE IF NOT EXISTS temperatures(
    country_code text,
    dt date,
    year int,
    average_temp double precision,
    average_temp_uncertainty double precision
);"""

currencies_create = """CREATE TABLE IF NOT EXISTS currencies(
    currency_code text,
    to_dollars numeric(20, 10),
    dt date
);"""

industries_create = """CREATE TABLE IF NOT EXISTS industries(
    industry_id text,
    description text
);"""

# --- STAGING TABLES ---

staging_emissions_copy = """
    copy staging_emissions from '{}'
    credentials 'aws_iam_role={}'
    region '{}';
""".format(config.get('S3', 'EMISSIONS_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'REGION'))

staging_temperatures_copy = """
    copy staging_temperatures from '{}'
    credentials 'aws_iam_role={}'
    region '{}';
""".format(config.get('S3', 'TEMPERATURES_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'REGION'))

staging_industrial_production_copy = """
    copy staging_industrial_production from '{}'
    credentials 'aws_iam_role={}'
    region '{}';
""".format(config.get('S3', 'INDUSTRIAL_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'REGION'))

staging_currencies_copy = """
    copy staging_currencies from '{}'
    credentials 'aws_iam_role={}'
    region '{}';
""".format(config.get('S3', 'CURRENCIES_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'REGION'))


# --- FINAL TABLES ---

industrial_production_insert = """
INSERT INTO industrial_production(country_code, industry_id, year, currency_code, value)
    SELECT country_code, industry_id, year, currency_code, value * POWER(10, power_code_code)
    FROM staging_industrial_production
    WHERE value_type_code = 'VALU'
;"""

countries_insert = """
INSERT INTO countries(country_code, year, emissions)
    SELECT country_code, year, emissions
    FROM staging_emissions
    WHERE country_code IS NOT NULL
        AND country_code != 'OWID_WRL'
;"""

temperatures_insert = """
WITH countries as(
    SELECT DISTINCT country, country_code 
    FROM staging_emissions
    WHERE country_code IS NOT NULL
        AND country_code != 'OWID_WRL'
)
INSERT INTO temperatures(country_code, dt, year, average_temp, average_temp_uncertainty)
    SELECT c.country_code, t.dt, EXTRACT(y from t.dt), t.avg_temp , t.avg_temp_uncertainty
    FROM staging_temperatures t
    LEFT JOIN countries c
        ON c.country = t.country
    WHERE c.country_code IS NOT NULL
;"""

# unnest solution from:
# https://stackoverflow.com/questions/1128737/unpivot-and-postgresql

names_array = "['AUD', 'EUR', 'NZD', 'GBP', 'BRL', 'CAD', 'CNY', 'HKD', 'INR', 'KRW', " \
"'MXN', 'ZAR', 'SGD', 'DKK', 'JPY', 'MYR', 'NOK', 'SEK', 'LKR', 'CHF', 'TWD', 'THB']"

cols_array = "[AUD, EUR, NZD, GBP, BRL, CAD, CNY, HKD, INR, KRW, " \
"MXN, ZAR, SGD, DKK, JPY, MYR, NOK, SEK, LKR, CHF, TWD, THB]"

currencies_insert = f"""
INSERT INTO currencies(currency_code, dt, to_dollars)
    SELECT unnest(array{names_array}),
           dt,
           unnest(array{cols_array})
    FROM staging_currencies
;"""

industries_insert = """
INSERT INTO industries(industry_id, description)
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