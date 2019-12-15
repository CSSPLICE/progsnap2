import csv
import sqlite3
import sys
import os
from io import TextIOWrapper
from zipfile import ZipFile
import json

input_zip_path = sys.argv[1]
output_path = sys.argv[2]

# TODO: Okay seriously make this a safer thing
if os.path.exists(output_path):
    os.remove(output_path)

def insert_csv(cur, input_zip_file, table_path, table_name):
    with input_zip_file.open(table_path, 'r') as table_file:
        reader = csv.reader(TextIOWrapper(table_file, 'utf-8'))
        header = next(reader)
        header_text = ", ".join(map(repr, header))
        cur.execute("CREATE TABLE {} ({});".format(table_name, header_text))
        rows = [row for row in reader]
        spots = ", ".join("?" for column in header)
        cur.executemany("INSERT INTO {} ({}) VALUES ({});".format(table_name, header_text, spots), rows)


def create_link_tables(cur, input_zip_file):
    cur.execute("CREATE TABLE LinkTable (Name);")
    names = []
    for link_table in input_zip_file.infolist():
        if link_table.filename.startswith('LinkTables/'):
            table_name = "Link" + link_table.filename[len("LinkTables/"):-len(".csv")]
            insert_csv(cur, input_zip_file, link_table.filename, table_name)
            names.append((table_name,))
    cur.executemany("INSERT INTO LinkTable (Name) VALUES (?);", names)


def create_code_state_tables(cur, input_zip_file):
    cur.execute("CREATE TABLE CodeState (ID, Filename, Contents);")
    code_states = []
    for code_state in input_zip_file.infolist():
        if code_state.filename.startswith('CodeStates/'):
            _, code_state_id, path = code_state.filename.split("/", maxsplit=2)
            with input_zip_file.open(code_state.filename, 'r') as code_file:
                code_states.append((code_state_id, path, code_file.read()))
    cur.executemany("INSERT INTO CodeState (ID, Filename, Contents) VALUES (?, ?, ?)", code_states)


con = sqlite3.connect(output_path)
cur = con.cursor()

with ZipFile(input_zip_path, 'r') as input_zip_file:
    # Create MainTable.csv table
    insert_csv(cur, input_zip_file, 'MainTable.csv', 'MainTable')
    # Create DatasetMetadata.csv table
    insert_csv(cur, input_zip_file, 'DatasetMetadata.csv', 'DatasetMetadata')
    # Create LinkTables table and individual Link* tables
    create_link_tables(cur, input_zip_file)
    # Create CodeStates table
    create_code_state_tables(cur, input_zip_file)

con.commit()
con.close()