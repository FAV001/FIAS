#Загружаем из файлов архивов ФИАС информацию в базу
#учитываем полную и дельта
from configobj import ConfigObj
import sqlite3
from sqlite3 import Error
import sys

def main():
    pass

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return None

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def check_db(database):
    sql_create_region_table = """CREATE TABLE IF NOT EXISTS region (
                                id INTEGER  PRIMARY KEY AUTOINCREMENT UNIQUE,
                                name TEXT,
                                short TEXT,
                                cod CHAR (2),
                                use BOOLEAN,
                                full_base DATE,
                                last_update DATE
                            );"""
    conn = create_connection(database)
    if conn is not None:
        create_table(conn, sql_create_region_table)
    else:
        print("Error! cannot create the database connection.")
        sys.exit(-1)


if __name__ == '__main__':
    check_db(".\\DB\\config.sqlite")
    conn = create_connection(".\\DB\\config.sqlite")
    with conn:
        cur = conn.cursor()
        sql = "select * from region where use=1 order by name"
        cur.execute(sql)
        while True:
            row = cur.fetchone()
            if row == None:
                break
            region_name = row[1]
            region_cod = row[3]
            full_base_date = row[5]
            delta_base_date = row[6]
            print(row)
