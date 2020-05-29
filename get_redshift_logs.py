import configparser
import psycopg2
from sys import exit
from os import remove
from datetime import datetime

def main():
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

    ts = int(datetime.now().timestamp())

    # --- Get Error Logs ---
    try:
        # Get history of all Amazon Redshift load errors
        cur.execute("SELECT * FROM stl_load_errors;")
        with open(f"logs/errorlog_{ts}.csv", "w") as log:
            for row in cur:
                for col in row:
                    log.write(f"{str(col)}, ")
                log.write("\n")

        # Get additional details of the loading error
        cur.execute("SELECT * FROM stl_loaderror_detail;")
        with open(f"logs/detailed_errorlog_{ts}.csv", "w") as log:
            for row in cur:
                for col in row:
                    log.write(f"{str(col)}, ")
                log.write("\n")
        print("Log files written.")
    except Exception as e:
        print("Log files could not be written")
        print(e)
    
    # --- Close Connection ---
    conn.close()


if __name__ == "__main__":
    main()