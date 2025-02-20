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
        self.iu = IU.ImportUtilities()
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
    def prepare_worksheet(self, content_model, output_file):
        details = self.iu.get_worksheet_details(self.namespace, content_model)
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            for detail in details:
                mods = self.iu.extract_from_mods(detail['field_pid'])
                row = mods | detail
                node_id = self.iu.get_nid_from_pid(self.namespace, row['field_member_of'])
                row['id'] = row['field_pid']
                dc = self.iu.get_dc_values(detail['field_pid'], self.namespace)
                if 'title' not in row:
                    row['title'] = dc['title']
                if node_id:
                    row['field_member_of'] = node_id
                    writer.writerow(row)




MP = ImportProcessor('ivoices')
