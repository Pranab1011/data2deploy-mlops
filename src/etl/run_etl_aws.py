from etl.etl_utils import *


def main():
    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )
    cursor = conn.cursor()

    cursor.execute(f"""select count(*) from {raw_table_name}""")
    if cursor.fetchone()[0] == 0:
        backfill_90days()

    else:
        monthly_insert()
        monthly_upsert()


if __name__ == "__main__":
    main()
