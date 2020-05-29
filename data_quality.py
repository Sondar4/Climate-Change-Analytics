import sys
import configparser
import psycopg2
from datetime import datetime

"""
This module executes Data Quality checks on the final tables. The output is both
printed in the console and saved as a log file in the logs folder.

It runs the following data quality checks:
    - Check that all tables have more than 9 rows.
    - Check that all currencies have a valid exchange rate.
    - Check that all country codes in `industrial_production` are in `temperatures`. 
    - Check that all descriptions in `industries` are filled.

Additional options:
    - If -v is passed as sys argument then logs are printed on terminal too.
"""

TABLES = [
    'industrial_production', 'countries', 'temperatures', 'currencies', 'industries'
]


def not_empty(cur, conn, v=False):
    """Checks that all final tables have at least 10 rows.
    """
    logs = []
    print("Checking that all tables have at least 10 rows...")
    for t in TABLES:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            conn.commit()
            row = cur.fetchone()
        except:
            raise ValueError(f"Something went wrong executing `not_empty` query on table {t}")

        if row == None:
            text = f"!!Data quality check on table {t} failed, no data fetched."
        elif row[0] <= 10:
            text = f"!!Data quality check on table {t} failed with {row[0]} rows."
        else:
            text = f"Data quality check on table {t} passed with {row[0]} rows."
        if v: print(text)
        logs.append(f"{text}\n")

    return logs


def valid_exchange_rate(cur, conn, v=False):
    """Checks that all currencies have exchange rate with non negative values.
    """
    logs = []
    print("Checking that all currencies have a valid exchange rate...")
    t = 'currencies'
    c = 'to_dollars'
    query = f"""
        SELECT COUNT(*) FROM {t}
            WHERE {c} <= 0
                OR {c} IS NULL
    """
    try:
        cur.execute(query)
        conn.commit()
        row = cur.fetchone()
    except:
        raise ValueError(f"Something went wrong executing `valid_exchange_rate` query on table {t}")

    if row == None:
        text = f"!!Data quality check on table {t} failed, no data fetched."
    elif row[0] > 0:
        text = f"!!Data quality check on table {t} failed, it contains {row[0]}" \
               f" non valid values in `{c}` column."
    else:
        text = f"Data quality check on table {t} column {c} passed, all values are valid."

    if v: print(text)
    logs.append(f"{text}\n")

    return logs


def country_codes_check(cur, conn, v=False):
    """Checks that all country codes in `industrial_production` are in `temperatures`.
    """
    logs = []
    print("Checking that all country codes in `industrial_production` are in `temperatures`...")
    # We want to Select the rows of industrial_production that
    # don't have a match on temperatures
    t1, t2 = 'industrial_production', 'temperatures'
    c1, c2 = 'country_code', 'country_code'
    query = f"""
        SELECT COUNT(*)
            FROM (SELECT DISTINCT {c1} FROM {t1}) as i
            LEFT JOIN (SELECT DISTINCT {c2} FROM {t2}) as t
                ON i.{c1} = t.{c2}
            WHERE t.{c2} IS NULL
    """
    try:
        cur.execute(query)
        conn.commit()
        row = cur.fetchone()
    except:
        raise ValueError(f"Something went wrong executing `country_codes_check` query on table {t1} JOIN {t2}")

    if row == None:
        text = f"!!Data quality check on table {t1} JOIN {t2} failed, no data fetched."
    elif row[0] > 0:
        text = f"!!Data quality check on table {t1} JOIN {t2} failed, it contains {row[0]}" \
               f" non matching values of `{c1}` column."
    else:
        text = f"Data quality check on table {t1} JOIN {t2} column {c1} passed, all codes match."

    if v: print(text)
    logs.append(f"{text}\n")

    return logs


def descriptions_filled(cur, conn, v=False):
    """Check that all descriptions in `industries` are filled.
    """
    logs = []
    print("Checking that all descriptions in `industries` are filled...")
    t = 'industries'
    c = 'description'
    query = f"SELECT COUNT(*) FROM {t} WHERE {c} IS NULL"
    try:
        cur.execute(query)
        conn.commit()
        row = cur.fetchone()
    except:
        raise ValueError(f"Something went wrong executing `descriptions_filled` query on table {t}")

    if row == None:
        text = f"!!Data quality check on table {t} failed, no data fetched."
    elif row[0] > 0:
        text = f"!!Data quality check on table {t} failed, it contains {row[0]}" \
               f" non valid values in `{c}` column."
    else:
        text = f"Data quality check on table {t} column {c} passed, all values are valid."

    if v: print(text)
    logs.append(f"{text}\n")

    return logs


def main(option=None):
    verbose=False
    if option == '-v': verbose=True

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # --- Establish Connection ---
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        print("Connection established.")
    except Exception as e:
        print("Something went wrong trying to connect to the cluster:")
        print(e)
        exit()

    # --- Run Data Quality Checks ---
    print("Running data quality checks...")
    logs = []
    logs += not_empty(cur, conn, verbose)
    logs += valid_exchange_rate(cur, conn, verbose)
    logs += country_codes_check(cur, conn, verbose)
    logs += descriptions_filled(cur, conn, verbose)

    # --- Write Logs File ---
    ts = int(datetime.now().timestamp())
    with open(f'logs/dataquality_{ts}', 'w') as fout:
        for l in logs:
            fout.write(l)

    # --- Close Connection ---
    print("All quality checks executed, log saved in the logs folder.")
    conn.close()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()