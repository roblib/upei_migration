#!/usr/bin/env python3

import ImportUtilities as IU
import ImportServerUtilities as SU
import sqlite3
import FoxmlWorker as FW
from pathlib import Path
import csv


class MigrationPrepper:
    def __init__(self, namespace):
        self.namespace = namespace
        self.objectStore = '/usr/local/fedora/data/objectStore'
        self.datastreamStore = '/usr/local/fedora/data/datastreamStore'
        self.conn = sqlite3.connect(f'{namespace}.db')
        self.conn.row_factory = sqlite3.Row
        self.su = SU.ImportServerUtilities(namespace)
        self.iu = IU.ImportUtilities(self.namespace)


    # Harvests the structure of all objects in a namespace and persists them to a database.
    def get_structure(self, collections=None):
        namespaces = [self.namespace]
        if collections:
            namespaces.extend(collections)
        cursor = self.conn.cursor()
        cursor.execute(f"""
            CREATE TABLE if not exists {self.namespace}(
            title TEXT,
            pid TEXT PRIMARY KEY,
            nid TEXT,
            content_model TEXT,
            collection_pid TEXT,
            page_of TEXT,
            sequence TEXT,
            constituent_of TEXT,
            dublin_core TEXT,
            mods TEXT
            )""")
        self.conn.commit()
        for namespace in namespaces:
            pids = self.su.get_pids_from_objectstore(namespace)
            for pid in pids:
                foxml_file = self.iu.dereference(pid)
                foxml = f"{self.objectStore}/{foxml_file}"
                fw = None
                if foxml:
                    try:
                        fw = FW.FWorker(foxml)
                    except (ValueError, RuntimeError) as e:
                        print(f"Skipping {foxml}: {e}")
                        fw = None
                if (fw):
                    if fw.get_state() != 'Active':
                        continue
                    relations = fw.get_rels_ext_values()
                    mapping = fw.get_file_data()
                    mods_info = mapping.get('MODS')
                    if mods_info:
                        mods_path = f"{self.datastreamStore}/{self.iu.dereference(mods_info['filename'])}"
                        mods_xml = Path(mods_path).read_text()
                    else:
                        mods_xml = fw.get_inline_mods()
                    if mods_xml:
                        mods_xml = mods_xml.replace("'", "''")
                    else:
                        mods_xml = ""
                    row = {
                        "title": fw.get_label(),
                        "pid": pid,
                        "nid": '',
                        "content_model": '',
                        "collection_pid": "",
                        "page_of": "",
                        "sequence": "",
                        "constituent_of": "",
                        "dublin_core": fw.get_dc(),
                        "mods": mods_xml
                    }

                    for relation, value in relations.items():
                        if relation in self.iu.rels_map:
                            row[self.iu.rels_map[relation]] = value
                    try:
                        command = f"""
                            INSERT OR REPLACE INTO {self.namespace} 
                            (title, pid, nid, content_model, collection_pid, page_of, sequence, constituent_of, dublin_core, mods) 
                            VALUES (:title, :pid, :nid,:content_model, :collection_pid, :page_of, :sequence, :constituent_of, :dublin_core, :mods)
                        """
                        cursor.execute(command, row)
                    except sqlite3.Error as e:
                        print(f"SQLite Error: {e}")
                        print(f"SQL Command: {command}")
                        print(f"Parameters: {row}")

                else:
                    print(f"FoXML file for {pid} is missing")
        self.conn.commit()

    # Prepares CSV for initial workbench ingest.
    def prepare_initial_ingest_worksheet(self, output_file):
        details = self.get_worksheet_details()
        if not details:
            print("No worksheet details found.")
            return

        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['id', 'title', 'field_pid', 'field_model', 'field_weight', 'file'])
            writer.writeheader()
            id = 1

            for detail in details:
                if not detail or not detail.get('field_pid'):  # Add safer checks for detail and field_pid
                    continue

                # Prepare the row for CSV
                row = {
                    'id': id,
                    'title': detail['title'],
                    'field_pid': detail['field_pid'],
                    'field_model': detail.get('field_model', 'Unknown'),
                    'field_weight': detail.get('field_weight', ''),
                }
                try:
                    writer.writerow(row)
                except Exception as e:
                    print(f"Failed to write row {row}: {e}")
                id += 1

    # Utility function to prepare database selections for workbench,
    def get_worksheet_details(self):
        command = f"Select * from {self.namespace}"
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

    # Map D7 values to D10 fields.
    def map_worksheet_values(self, line):
        map = {
            'content_model': 'field_model',
            'pid': 'field_pid',
            'collection_pid': 'field_member_of',
            'page_of': 'field_member_of',
            'sequence': 'field_weight',
            'title': 'title',
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
            cleaned_line['field_model'] = content_map[cleaned_line['field_model']]
        return cleaned_line

    def update_structure(self, collections=None):
        namespaces = [self.namespace]
        for namespace in namespaces:
            pids = self.su.get_pids_from_objectstore(namespace)
            for pid in pids:
                foxml_file = self.iu.dereference(pid)
                foxml = f"{self.objectStore}/{foxml_file}"
                fw = None
                if foxml:
                    try:
                        fw = FW.FWorker(foxml)
                    except (ValueError, RuntimeError) as e:
                        print(f"Skipping {foxml}: {e}")
                        fw = None
                if (fw):
                    if fw.get_state() != 'Active':
                        continue
                    relations = fw.get_rels_ext_values()
                    pipe = '|'
                    has_pipe = [k for k, v in relations.items() if pipe in v]
                    for key, value in has_pipe:
                        print(f"{pid},{value}")







if __name__ == '__main__':
    MP = MigrationPrepper('island_newspapers')
    collections = ['*']
    MP.get_structure(collections)