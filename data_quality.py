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
    """ Checks that all final tables have at least 10 rows.
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
            text = f"!!Data quality check on table {t} failed, it has no rows."
            if v: print(text)
            logs.append(f"{text}\n")
        elif row[0] <= 10:
            text = f"!!Data quality check on table {t} failed with {row[0]} rows."
            if v: print(text)
            logs.append(f"{text}\n")
        else:
            text = f"Data quality check on table {t} passed with {row[0]} rows."
            if v: print(text)
            logs.append(f"{text}\n")
    return logs


def valid_exchange_rate(cur, conn, v=False):
    #TODO: Write description str and finish function
    logs = []
    print("Checking that all currencies have a valid exchange rate...")
    print("This hasn't been implemented yet")
    return logs


def country_codes_check(cur, conn, v=False):
    #TODO: Write description str and finish function
    logs = []
    print("Checking that all country codes in `industrial_production` are in `temperatures`...")
    print("This hasn't been implemented yet")
    return logs


def descriptions_filled(cur, conn, v=False):
    #TODO: Write description str and finish function
    logs = []
    print("Checking that all descriptions in `industries` are filled...")
    print("This hasn't been implemented yet")
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
    ts = datetime.now().timestamp()
    with open(f'logs/dataquality_{ts}', 'w') as fout:
        for l in logs:
            fout.write(l)
    print("Logs file written.")

    # --- Close Connection ---
    print("All quality checks executed, all logs saved in the logs folder.")
    conn.close()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()