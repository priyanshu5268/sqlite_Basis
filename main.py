import pandas as pd
import sqlite3
import csv
import sys

connection = sqlite3.connect("name.db")
cursor = connection.cursor()
#table creation
try:
    cursor.execute("""CREATE TABLE{table_name} 
                                Column_1 char(20) not null,
                                Column_2 char(20) not null,
                                Column_3 char(20) not null,
                                Column_4 char(20) not null,
                                Column_5 char(20) not null,
                                Column_6 int not null,
                                Column_7 int not null,
                                Column_8 char(20) not null,
                                Column_9 char(20) not null,
                                Column_10 char(20) UNIQUE not null
                                """)
except:
    pass
#data insertion
try:
    with open(" ENTER FILE_PATH FOLDER") as file:
        records=0
        for row in file:
            cursor.execute("INSERT OR IGNORE INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row.split(", "))
            connection.commit()
            records += 1
    print('\n{} Records Transfer Complete'.format(records))
except:
    pass


#fetch
def getAllRows():
    try:
        print("Connected to SQLite")
        data = "select * from {table_name}"
        cursor.execute(data)
        records = cursor.fetchall()
        print(" Total rows are: ", len(records))
        for row in records[0:]:
            ("Column_1: ", row[0])
            ("Column_2: ", row[1])
            ("Column_3: ", row[2])
            ("Column_4: ", row[3])
            ("Column_5: ", row[4])
            ("Column_6: ", row[5])
            ("Column_7: ", row[6])
            ("Column_8: ", row[7])
            ("Column_9: ", row[8])
            ("Column_10: ", row[9])
    except sqlite3.Error as error:
        print("Failed to read data from table", error)
    finally:
        if connection:
            connection.close()
            print("The SQLite connection is closed")

getAllRows()
connection.close()

