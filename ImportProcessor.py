#!/usr/bin/env python3

import csv
import time

import FoxmlWorker as FW
import ImportServerUtilities as IS
import ImportUtilities as IU

"""
This class is used to prepare data for ingest into the UPEI ingestion platform.
"""


class ImportProcessor:

    def __init__(self, namespace):
        self.objectStore = '/usr/local/fedora/data/objectStore'
        self.datastreamStore = '/usr/local/fedora/data/datastreamStore'
        self.stream_map = {
            'islandora:sp_pdf': ['OBJ', 'PDF'],
            'islandora:sp_large_image_cmodel': ['OBJ'],
            'islandora:sp_basic_image': ['OBJ'],
            'ir:citationCModel': ['FULL_TEXT'],
            'ir:thesisCModel': ['OBJ', 'PDF', 'FULL_TEXT'],
            'islandora:sp_videoCModel': ['OBJ', 'PDF'],
            'islandora:newspaperIssueCModel': ['OBJ', 'PDF'],
            'islandora:sp-audioCModel': ['OBJ'],
        }
        self.iu = IU.ImportUtilities(namespace)
        self.ms = IS.ImportServerUtilities(namespace)
        self.namespace = namespace
        self.export_dir = '/opt/islandora/upei_migration/export'
        self.mimemap = {"image/jpeg": ".jpg",
                        "image/jp2": ".jp2",
                        "image/png": ".png",
                        "image/tiff": ".tif",
                        "text/xml": ".xml",
                        "text/plain": ".txt",
                        "application/pdf": ".pdf",
                        "application/xml": ".xml",
                        "audio/x-wav": ".wav",
                        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
                        "application/octet-stream": ".bib",
                        "audio/mpeg": ".mp3",
                        "video/mp4": ".mp4",
                        "video/x-m4v": ".m4v",
                        "audio/vnd.wave": '.wav'
                        }
        self.fieldnames = ['id', 'title', 'parent_id', 'field_member_of', 'field_edtf_date_issued', 'field_abstract',
                           'field_genre', 'field_subject', 'field_geographic_subject', 'field_physical_description',
                           'field_extent', 'field_resource_type', 'field_linked_agent', 'field_pid',
                           'field_related_item', 'field_edtf_date_other', 'field_edtf_copyright_date', 'field_issuance',
                           'field_location', 'field_publisher', 'field_edition', 'field_access_condition',
                           'field_model', 'field_edtf_date_created', 'file', 'field_subtitle', 'field_identifier',
                           'field_alternative_title']
        self.start = time.time()

    # Prepares workbench sheet for collection structure
    def prepare_collection_worksheet(self, output_file):
        collection_pids = self.iu.get_collection_pids(self.namespace)
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            rows = []
            for entry in collection_pids:
                pid = entry.get('field_pid')
                if self.namespace not in entry.get('field_member_of'):
                    continue
                mods = self.iu.extract_from_mods(pid)
                row = {'id': pid}
                all_fields = entry | mods
                for key, value in all_fields.items():
                    if type(value) is list:
                        value = '|'.join(value)
                    row[key] = value
                rows.append(row)
            processed = ['islandora:root']
            while rows:
                for row in rows:
                    if row.get('field_member_of') in processed:
                        writer.writerow(row)
                        rows.remove(row)
                        processed.append(row.get('id'))

    # Prepares ingest worksheets per collections
    def prepare_collection_member_worksheet(self, collections, output_file):
        details = self.iu.get_collection_member_details(self.namespace, collections)
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            for detail in details:
                mods = self.iu.extract_from_mods(detail['field_pid'])
                row = mods | detail
                writer.writerow(row)

    # Prepares worksheets for workbench ingest.
    def prepare_initial_ingest_worksheet(self, output_file):
        details = self.iu.get_worksheet_details()
        if not details:  # Check if details is None or empty
            print("No worksheet details found.")
            return

        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile,
                                    fieldnames=['id', 'title', 'field_title', 'field_pid', 'field_model', 'file'])
            writer.writeheader()
            id = 1

            for detail in details:
                if not detail or not detail.get('field_pid'):  # Add safer checks for detail and field_pid
                    continue

                # Fetch DC values and ensure it's valid
                dc = self.iu.get_dc_values(detail['field_pid'])
                if not dc or 'title' not in dc:  # Ensure dc is not None and has a 'title' key
                    print(f"Warning: Missing DC values for PID {detail['field_pid']}")
                    continue

                # Prepare the row for CSV
                row = {
                    'id': id,
                    'title': dc['title'],
                    'field_title': dc['title'],
                    'field_pid': detail['field_pid'],
                    'field_model': detail.get('field_model', 'Unknown')  # Use .get() with fallback for safety
                }
                try:
                    if 'web' in detail['field_pid']:
                        writer.writerow(row)
                except Exception as e:
                    print(f"Failed to write row {row}: {e}")
                id += 1

    def prepare_relationship_worksheet(self, output_file):
        relationships = self.iu.get_relationships(self.namespace)
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['node_id', 'title', 'field_member_of'])
            writer.writeheader()
            for relationship in relationships:
                row = {}
                if not relationship['node_id']:
                    continue
                row['node_id'] = relationship['node_id']
                row['field_member_of'] = relationship['member_of']
                writer.writerow(row)





    def full_server_prep(self):
        self.ms.build_record_from_pids(self.namespace)


MP = ImportProcessor('bdh')
MP.prepare_relationship_worksheet('worksheets/new_relations.csv')
