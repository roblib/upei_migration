#!/usr/bin/env python3

import csv
import hashlib
import sqlite3
import urllib
import urllib.parse
from typing import Any

import lxml.etree as ET
import re
import time
import functools
import pickle
import ModsTransformer as MT


class ImportUtilities:
    def __init__(self, namespace):
        self.conn = sqlite3.connect(f'{namespace}.db')
        self.conn.row_factory = sqlite3.Row
        self.fields = ['PID', 'model', 'RELS_EXT_isMemberOfCollection_uri_ms', 'RELS_EXT_isPageOf_uri_ms']
        self.objectStore = '/usr/local/fedora/data/objectStore/'
        self.datastreamStore = '/usr/local/fedora/data/datastreamStore/'
        self.rels_map = {
            'isMemberOfCollection': 'collection_pid',
            'isMemberOf': 'collection_pid',
            'hasModel': 'content_model',
            'isPageOf': 'page_of',
            'isSequenceNumber': 'sequence',
            'isConstituentOf': 'constituent_of',
            'mods': 'mods'
        }
        self.namespace = namespace

    def human_readable_time(seconds):
        """Convert seconds to a human-readable format (hours, minutes, seconds, milliseconds)."""
        hours, remainder = divmod(seconds, 3600)
        minutes, remainder = divmod(remainder, 60)
        seconds, milliseconds = divmod(remainder, 1)
        milliseconds = int(milliseconds * 1000)
        parts = []
        if hours:
            parts.append(f"{int(hours)}h")
        if minutes:
            parts.append(f"{int(minutes)}m")
        if seconds or (not parts and milliseconds == 0):  # Always show seconds if no larger unit exists
            parts.append(f"{int(seconds)}s")
        if milliseconds:
            parts.append(f"{milliseconds}ms")
        return " ".join(parts)

    def timeit(func):
        """Decorator to measure the execution time of a function in a human-readable format."""

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):  # Ensure 'self' is passed for instance methods
            start_time = time.time()
            result = func(self, *args, **kwargs)  # Call the method with 'self'
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Function '{func.__name__}' executed in: {ImportUtilities.human_readable_time(elapsed_time)}")
            return result

        return wrapper

    # Adds node_id to table
    @timeit
    def add_node_ids(self, table, csv_file):
        cursor = self.conn.cursor()
        with open(csv_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                command = f"UPDATE {table} SET nid = ? WHERE pid = ?"
                cursor.execute(command, (row['ID'], row['PID']))
        self.conn.commit()

    # Adds node_id to table
    @timeit
    def add_dc_to_database(self, table, csv_file):
        cursor = self.conn.cursor()
        with open(csv_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                command = f"UPDATE {table} SET dublin_core = ? WHERE pid = ? and dublin_core is NULL"
                cursor.execute(command, (row['dublin_core'], row['pid']))
        self.conn.commit()

    # Identifies object and datastream location within Fedora objectStores and datastreamStore.
    def dereference(self, identifier: str) -> str:
        # Replace '+' with '/' in the identifier
        slashed = identifier.replace('+', '/')
        full = f"info:fedora/{slashed}"
        # Generate the MD5 hash of the full string
        hash_value = hashlib.md5(full.encode('utf-8')).hexdigest()
        # Pattern to fill with hash (similar to the `##` placeholder)
        subbed = "##"
        # Replace the '#' characters in `subbed` with the corresponding characters from `hash_value`
        hash_offset = 0
        pattern_offset = 0
        result = list(subbed)

        while pattern_offset < len(result) and hash_offset < len(hash_value):
            if result[pattern_offset] == '#':
                result[pattern_offset] = hash_value[hash_offset]
                hash_offset += 1
            pattern_offset += 1

        subbed = ''.join(result)
        # URL encode the full string, replacing '_' with '%5F'
        encoded = urllib.parse.quote(full, safe='').replace('_', '%5F')
        return f"{subbed}/{encoded}"

    # Gets all pages from book
    def get_pages(self, table, book_pid):
        cursor = self.conn.cursor()
        command = f"SELECT PID from {table} where page_of = '{book_pid}'"
        pids = []
        for row in cursor.execute(command):
            pids.append(row[0])
        return pids

    # Gets all books in the repository.
    def get_books(self, table, collection):
        cursor = self.conn.cursor()
        command = f"SELECT PID, CONTENT_MODEL from {table} where collection_pid = '{collection}' AND CONTENT_MODEL = 'islandora:bookCModel' "
        pids = []
        for row in cursor.execute(command):
            pids.append(row[0])
        return pids

    # Processes CSV returned from direct objectStore harvest
    def process_full_institution(self, csv_file, table):
        cursor = self.conn.cursor()
        cursor.execute(f"""
            CREATE TABLE if not exists {table}(
            title TEXT,
            pid TEXT PRIMARY KEY,
            nid TEXT,
            content_model TEXT,
            collection_pid TEXT,
            page_of TEXT,
            sequence TEXT,
            constituent_of TEXT
            dublin_core TEXT,
            mods TEXT
            )""")
        self.conn.commit()
        with open(csv_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    command = f"""
                        INSERT OR REPLACE INTO {table} 
                        (title, pid, content_model, collection_pid, page_of, sequence, constituent_of) 
                        VALUES (:title, :pid, :content_model, :collection_pid, :page_of, :sequence, :constituent_of)
                    """
                    cursor.execute(command, row)
                except sqlite3.Error as e:
                    print(f"SQLite Error: {e}")  # Print the error message
                    print(f"SQL Command: {command}")  # Optional: Print the SQL command for debugging
                    print(f"Parameters: {row}")
        self.conn.commit()

    # Get all collection contents within namespace
    def get_collection_content_pids(self, table, collection, filename):
        collection_pids = [collection]
        parent_types = ['islandora:bookCModel', 'islandora:collectionCModel', ]
        pids = []
        cursor = self.conn.cursor()
        while collection_pids:
            collection = collection_pids.pop()
            command = f"SELECT * from {table} where collection_pid = '{collection}'"
            for row in cursor.execute(command):
                if row['content_model'] in parent_types:
                    collection_pids.append(row['pid'])
                pids.append(row['pid'])
        with open(filename, 'wb') as file:
            pickle.dump(pids, file)

    # Utility function to prepare database selections for workbench
    def get_worksheet_details(self, content_model=None):
        command = f"Select * from {self.namespace}"
        if content_model is not None:
            command = f"{command} where content_model = '{content_model}'"

        cursor = self.conn.cursor()
        details = []
        for row in cursor.execute(command):
            keys = row.keys()
            line = {}
            for key in keys:
                line[key] = (row[key])
            cleaned_line = self.map_worksheet_values(line)
            details.append(cleaned_line)
        return details

    # Map D7 values to D10
    def map_worksheet_values(self, line):
        map = {
            'content_model': 'field_model',
            'pid': 'field_pid',
            'collection_pid': 'field_member_of',
            'page_of': 'field_member_of',
            'sequence': 'field_weight',
        }
        content_map = {
            'islandora:collectionCModel': 'Collection',
            'islandora:sp_large_image_cmodel': 'Image',
            'islandora:sp-audioCModel': 'Audio',
            'islandora:pageCModel': 'Page',
            'islandora:bd_pageCModel': 'Page',
            'islandora:bookCModel': 'Paged Content',
            'islandora:compoundCModel': 'Compound Object',
            'islandora:sp_pdf': 'Digital Document',
            'islandora:sp_basic_image': 'Image',
            'islandora:newspaperCModel': 'Newspaper',
            'islandora:newspaperIssueCModel': 'Publication Issue',
            'islandora:newspaperPageCModel': 'Page',
            'islandora:oralhistoriesCModel': 'Compound Object',
            'islandora:sp_videoCModel': 'Video',
            'ir:thesisCModel': 'Digital Document',
            'islandora:rootSerialCModel': 'Compound Object',
            'islandora:intermediateCModel': 'Compound Object',
            'ir:citationCModel': 'Citation',
            'islandora:audioCModel|islandora:sp-audioCModel': 'Audio',
            'islandora:slideCModel|islandora:sp_large_image_cmodel': 'Image',
            'islandora:videoCModel|islandora:sp_videoCModel': 'Video',
            'islandora:audioCModel': 'Audio',

        }

        cleaned_line = {}
        for key, value in line.items():
            if key in map:
                if value is None:
                    value = ''
                if value.strip():
                    cleaned_line[map[key]] = value
        if 'field_model' in cleaned_line:
            cleaned_line['field_model'] = content_map.get(cleaned_line['field_model'])

        return cleaned_line

    # Get all content models from map
    def get_collection_pid_model_map(self, table, collection):
        cursor = self.conn.cursor()
        command = f"SELECT PID, CONTENT_MODEL from {table} where collection_pid = '{collection}'"
        map = {}
        for row in cursor.execute(command):
            map[row[0]] = row[1]
        return map

    def get_subcollections(self, table, collection):
        cursor = self.conn.cursor()
        command = f"SELECT PID, CONTENT_MODEL from {table} where collection_pid = '{collection}' AND CONTENT_MODEL = 'islandora:collectionCModel' "
        pids = []
        for row in cursor.execute(command):
            pids.append(row[0])
        return pids

    def get_collection_recursive_pid_model_map(self, table, collection_pid):
        descendants = {}
        cursor = self.conn.cursor()
        command = f"select PID, CONTENT_MODEL from {table} where COLLECTION_PID = '{collection_pid}'"
        child_collections = []
        books = []
        for row in cursor.execute(command):
            if row['content_model'] in ['islandora:collectionCModel', 'islandora:bookCModel']:
                child_collections.append(row['PID'])
                descendants[row['PID']] = row['content_model']
            else:
                descendants[row['PID']] = row['content_model']
        while child_collections:
            child_collection = child_collections.pop(0)
            command = f"select PID, CONTENT_MODEL from {table} where COLLECTION_PID = '{child_collection}' or page_of = '{child_collection}' "
            for row in cursor.execute(command):
                if row['content_model'] in ['islandora:collectionCModel', 'islandora:bookCModel']:
                    child_collections.append(row['PID'])
                    descendants[row['PID']] = row['content_model']
                else:
                    descendants[row['PID']] = row['content_model']
        return descendants

    def extract_from_mods(self, pid):
        cursor = self.conn.cursor()
        command = f"SELECT MODS from MSVU where PID = '{pid}'"
        result = cursor.execute(command).fetchone()
        mods = result['MODS']
        if mods is not None and len(mods) < 10:
            return {}
        return self.mt.extract_from_mods(mods)

    # Get node_id associated with pid.
    def get_nid_from_pid(self, table, pid):
        cursor = self.conn.cursor()
        command = f"SELECT nid from {table} where PID = '{pid}'"
        result = cursor.execute(command).fetchone()
        return result['nid'] if result is not None else ''

    # Get pid associated with nid.
    def get_pid_from_nid(self, table: object, nid: object) -> str | Any:
        cursor = self.conn.cursor()
        command = f"SELECT pid from {table} where nid = '{nid}'"
        result = cursor.execute(command).fetchone()
        return result['pid'] if result is not None else ''

    # Get all pids with content model.
    def get_pids_by_content_model(self, table, content_model):
        cursor = self.conn.cursor()  # Manually create a cursor
        try:
            command = f"SELECT PID FROM {table} WHERE CONTENT_MODEL LIKE ?"
            cursor.execute(command, (f"%{content_model}%",))
            return [row[0] for row in cursor.fetchall()]  # Extracts all PID values
        finally:
            cursor.close()  # Ensures the cursor is always closed

    # Get key - value pairs from stored dublin core.
    def get_dc_values(self, pid):
        cursor = self.conn.cursor()
        result = cursor.execute(f"select dublin_core from {self.namespace} where pid = '{pid}'")
        dc = result.fetchone()['dublin_core']
        if dc:
            root = ET.fromstring(dc)
            namespaces = {
                'dc': 'http://purl.org/dc/elements/1.1/'
            }
            tags_and_values = [(elem.tag, elem.text) for elem in root.findall('.//dc:*', namespaces)]
            dc_vals = {}
            for tag, value in tags_and_values:
                tag = re.sub(r"\{.*?\}", "", tag)
                dc_vals[tag] = value
            return dc_vals

    def add_title(self):
        cursor = self.conn.cursor()
        command = f"SELECT PID, title from {self.namespace} where title is null"
        pids = cursor.execute(command).fetchall()
        for pid in pids:
            dc = self.get_dc_values(pid['PID'])
            query = f"UPDATE {self.namespace} SET title = ? WHERE pid = ?"
            values = (dc.get('title'), pid['PID'])
            cursor.execute(query, values)
        self.conn.commit()

    def get_relationships(self, table):
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT child.nid, parent.nid, child.sequence, child.title
            FROM {table} as child
            JOIN {table} as parent
            ON child.collection_pid = parent.pid
        """)
        rows = cursor.fetchall()
        relationships = [
            {'node_id': r[0], 'member_of': r[1], 'weight': r[2] if r[2] is not None else '', 'title': r[3]}
            for r in rows
        ]
        return relationships

    import csv

    def make_media_add_worksheet(self, input_file, output_file):
        with open(output_file, mode="w", newline="") as out_file:
            writer = csv.DictWriter(out_file, fieldnames=['node_id', 'file', 'media_use_tid'])
            writer.writeheader()
            with open(input_file, "r") as file:
                for line in file:
                    row = {
                        'node_id': line.split('_')[0],
                        'file': line.strip()
                    }
                    if 'MODS' in line:
                        row['media_use_tid'] = 57
                    if 'PBCORE' in line:
                        row['media_use_tid'] = 56
                    writer.writerow(row)

    def make_archive_url_worksheet(self, output_file):
        cursor = self.conn.cursor()
        statement = f"select pid, nid from {self.namespace} where pid like '%batch%'"
        cursor.execute(statement)
        results = cursor.fetchall()
        with open(output_file, mode="w", newline="") as out_file:
            writer = csv.DictWriter(out_file, fieldnames=['node_id', 'field_archival_alias'])
            writer.writeheader()
            for result in results:
                row = {}
                row['node_id'] = result['nid']
                row['field_archival_alias'] = f"islandora:{result['pid'][12:]}"
                writer.writerow(row)
    def fix_media(self, infile, outfile):
        with open(infile, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            pids = []

            for row in reader:
                nid = row['Media name'][:2]
                pids.append(self.get_pid_from_nid('ivoices', nid))

            print(pids)

    def redwhite(self, infile, outfile):
        with open(outfile, mode="w", newline="") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=['id','title','field_pid','field_model','field_weight','file'])
            writer.writeheader()
            with open(infile, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if row['field_pid'] == 'vre:redwhite' or 'vre:rw-batch' in row['field_pid']:
                        writer.writerow(row)



    def build_new_bdh(self, infile, outfile):
        with open(outfile, mode="w", newline="") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=['node_id', 'field_member_of'])
            writer.writeheader()
            with open(infile, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    node_id = self.get_nid_from_pid('bdh', row['pid'])
                    collection_pids = []
                    collections = row['collections'].split('|')
                    for collection in collections:
                        collection_pids.append(self.get_nid_from_pid('bdh', collection))
                    collection_string = '|'.join(collection_pids)
                    new_row = {'node_id': node_id, 'field_member_of': collection_string}
                    writer.writerow(new_row)

    def prepare_restricted_worksheet(self, input_file, output_file):
        pids = []
        with open(input_file, "r") as f:
            for line in f:
                pids.append(line.strip())
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['node_id', 'published'])
            writer.writeheader()
            for pid in pids:
                row = {}
                nid = self.get_nid_from_pid('bdh', pid)
                if nid is not None:
                    row['node_id'] = nid
                    row['published'] = 0
                    writer.writerow(row)


if __name__ == '__main__':
    MU = ImportUtilities('bdh')
    MU.prepare_restricted_worksheet('assets/bdh_restricted_pids.csv', 'outputs/bdh_restricted_pids.csv')
