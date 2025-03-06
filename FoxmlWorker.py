import lxml.etree as ET

"""
FoxmlWorker.py encapsulates the Foxml object and provides methods to extract data from it.
"""


class FWorker:
    def __init__(self, foxml_file):
        try:
            self.tree = ET.parse(foxml_file)
            self.root = self.tree.getroot()
        except ET.ParseError as e:
            raise ValueError(f"Error: Unable to parse FOXML file '{foxml_file}'. XML may be malformed. Details: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while parsing FOXML file '{foxml_file}': {e}")
        self.namespaces = {
            'foxml': 'info:fedora/fedora-system:def/foxml#',
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'fedora': "info:fedora/fedora-system:def/relations-external#",
            'fedora-model': "info:fedora/fedora-system:def/model#",
            'islandora': "http://islandora.ca/ontology/relsext#",
            'mods': 'http://www.loc.gov/mods/v3'
        }
        self.properties = self.get_properties()

    # Returns PID from foxml
    def get_pid(self):
        return self.root.attrib['PID']

    # Gets state
    def get_state(self):
        return self.properties['state']

    # Gets label
    def get_label(self):
        return self.properties['label']

    # Gets all properties.
    def get_properties(self):
        values = {}
        properties = self.root.findall('.//foxml:objectProperties/foxml:property', self.namespaces)
        for property in properties:
            name = property.attrib['NAME'].split('#')[1]
            value = property.attrib['VALUE']
            values[name] = value
        return values

    # Gets all datastream types from foxml.
    def get_datastream_types(self):
        ns = {'': 'info:fedora/fedora-system:def/foxml#'}
        datastreams = self.root.findall('.//foxml:datastream', self.namespaces)
        types = {}
        for datastream in datastreams:
            versions = datastream.findall('./foxml:datastreamVersion', self.namespaces)
            mimetype = versions[-1].attrib['MIMETYPE']
            types[datastream.attrib['ID']] = mimetype
        return types

    # Gets names of current managed files from foxml.
    def get_file_data(self):
        mapping = {}
        streams = self.get_datastream_types()
        for stream, mimetype in streams.items():
            location = self.root.xpath(
                f'//foxml:datastream[@ID="{stream}"]/foxml:datastreamVersion/foxml:contentLocation',
                namespaces=self.namespaces)
            if location:
                mapping[stream] = {'filename': location[-1].attrib['REF'], 'mimetype': mimetype}
        return mapping

    # Returns dc stream as XML
    def get_dc(self):
        dc_nodes = self.root.findall(
            f'.//foxml:datastream[@ID="DC"]/foxml:datastreamVersion/foxml:xmlContent/oai_dc:dc',
            namespaces=self.namespaces)
        dc_node = dc_nodes[-1]
        return ET.tostring(dc_node, encoding='unicode')

    # Returns list of Dublin Core key/value pairs.  Allows for mulitples.
    def get_dc_values(self):
        dc_nodes = self.root.findall(f'.//foxml:datastream[@ID="DC"]/foxml:datastreamVersion/foxml:xmlContent',
                                     namespaces=self.namespaces)
        dc_values = []
        if not dc_nodes:
            print(f"{self.get_pid()}: No DC values found.")
            return dc_values
        dc_node = dc_nodes[-1]
        for child in dc_node.iter():
            if child.text is not None:
                cleaned = child.text.replace('\n', '')
                text = ' '.join(cleaned.split())
                if text:
                    tag = child.xpath('local-name()')
                    dc_values.append({tag: text})
        return dc_values

    # Returns key/value pairs from RELS-EXT.
    def get_rels_ext_values(self):
        re_values = {}
        re_nodes = self.root.findall(
            f'.//foxml:datastream[@ID="RELS-EXT"]/foxml:datastreamVersion/foxml:xmlContent/rdf:RDF',
            namespaces=self.namespaces)
        if re_nodes is None:
            return re_values
        re_node = re_nodes[-1]
        for child in re_node.iter():
            tag = child.xpath('local-name()')
            if child.text is not None:
                cleaned = child.text.replace('info:fedora/', '').replace('\n', '')
                text = ' '.join(cleaned.split())
                if text:
                    re_values[tag] = text
            resource = child.attrib.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            if resource:
                re_values[tag] = resource.replace('info:fedora/', '')
        return re_values

    # Older Fedora objects may kep mods inline rather than ina separate file in the dataStore.
    def get_inline_mods(self):
        retval = ''
        try:
            mods_datastream = self.root.findall(
                ".//foxml:datastream[@ID='MODS']/foxml:datastreamVersion/foxml:xmlContent/mods:mods",
                self.namespaces
            )
            if not mods_datastream:
                return retval
            mods_node = mods_datastream[-1]
            if mods_node is not None:
                retval = ET.tostring(mods_node, encoding='unicode')

        except Exception as e:
            print(f"An error occurred: {e}")

        return retval


if __name__ == '__main__':
    FW = FWorker('assets/sample_fox.xml')

