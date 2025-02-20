import re
from datetime import datetime

import edtf_validate.valid_edtf
import xmltodict


class ModsTransformer:
    def __init__(self):
        self.summary = {}
        self.relator_map = {
            '': 'relators:att',
            'Abridger': 'relators:abr',
            'Actor': 'relators:act',
            'Adapter': 'relators:adp',
            'Addressee': 'relators:rcp',
            'Analyst': 'relators:anl',
            'Animator': 'relators:anm',
            'Annotator': 'relators:ann',
            'Appellant': 'relators:apl',
            'Appellee': 'relators:ape',
            'Applicant': 'relators:app',
            'Architect': 'relators:arc',
            'Arranger': 'relators:arr',
            'Art copyist': 'relators:acp',
            'Art director': 'relators:adi',
            'Artist': 'relators:art',
            'Artistic director': 'relators:ard',
            'Assignee': 'relators:asg',
            'Associated name': 'relators:asn',
            'Auctioneer': 'relators:auc',
            'Author': 'relators:aut',
            'Author in quotations or text abstracts': 'relators:aqt',
            'Author of afterword, colophon, etc.': 'relators:aft',
            'Author of dialog': 'relators:aud',
            'Author of introduction, etc.': 'relators:aui',
            'Autographer': 'relators:ato',
            'Bibliographic antecedent': 'relators:ant',
            'Binder': 'relators:bnd',
            'Binding designer': 'relators:bdd',
            'Blurb writer': 'relators:blw',
            'Book designer': 'relators:bkd',
            'Book producer': 'relators:bkp',
            'Bookjacket designer': 'relators:bjd',
            'Bookplate designer': 'relators:bpd',
            'Bookseller': 'relators:bsl',
            'Braille embosser': 'relators:brl',
            'Broadcaster': 'relators:brd',
            'Calligrapher': 'relators:cll',
            'Cartographer': 'relators:ctg',
            'Caster': 'relators:cas',
            'Censor': 'relators:cns',
            'Choreographer': 'relators:chr',
            'Collaborator (deprecated, use Contributor)': 'relators:clb',
            'Cinematographer': 'relators:cng',
            'Client': 'relators:cli',
            'Collection registrar': 'relators:cor',
            'Collector': 'relators:col',
            'Collotyper': 'relators:clt',
            'Colorist': 'relators:clr',
            'Commentator': 'relators:cmm',
            'Commentator for written text': 'relators:cwt',
            'Compiler': 'relators:com',
            'Complainant': 'relators:cpl',
            'Complainant-appellant': 'relators:cpt',
            'Complainant-appellee': 'relators:cpe',
            'Composer': 'relators:cmp',
            'Compositor': 'relators:cmt',
            'Conceptor': 'relators:ccp',
            'Conductor': 'relators:cnd',
            'Conservator': 'relators:con',
            'Consultant': 'relators:csl',
            'Consultant to a project': 'relators:csp',
            'Contestant': 'relators:cos',
            'Contestant-appellant': 'relators:cot',
            'Contestant-appellee': 'relators:coe',
            'Contestee': 'relators:cts',
            'Contestee-appellant': 'relators:ctt',
            'Contestee-appellee': 'relators:cte',
            'Contractor': 'relators:ctr',
            'Contributor': 'relators:ctb',
            'Copyright claimant': 'relators:cpc',
            'Copyright holder': 'relators:cph',
            'Corrector': 'relators:crr',
            'Correspondent': 'relators:crp',
            'Costume designer': 'relators:cst',
            'Court governed': 'relators:cou',
            'Court reporter': 'relators:crt',
            'Cover designer': 'relators:cov',
            'Creator': 'relators:cre',
            'Curator': 'relators:cur',
            'Dancer': 'relators:dnc',
            'Data contributor': 'relators:dtc',
            'Data manager': 'relators:dtm',
            'Dedicatee': 'relators:dte',
            'Dedicator': 'relators:dto',
            'Defendant': 'relators:dfd',
            'Defendant-appellant': 'relators:dft',
            'Defendant-appellee': 'relators:dfe',
            'Degree granting institution': 'relators:dgg',
            'Degree supervisor': 'relators:dgs',
            'Delineator': 'relators:dln',
            'Depicted': 'relators:dpc',
            'Depositor': 'relators:dpt',
            'Designer': 'relators:dsr',
            'Director': 'relators:drt',
            'Dissertant': 'relators:dis',
            'Distribution place': 'relators:dbp',
            'Distributor': 'relators:dst',
            'Donor': 'relators:dnr',
            'Draftsman': 'relators:drm',
            'Dubious author': 'relators:dub',
            'Editor': 'relators:edt',
            'Editor of compilation': 'relators:edc',
            'Editor of moving image work': 'relators:edm',
            'Electrician': 'relators:elg',
            'Electrotyper': 'relators:elt',
            'Enacting jurisdiction': 'relators:enj',
            'Engineer': 'relators:eng',
            'Engraver': 'relators:egr',
            'Etcher': 'relators:etr',
            'Event place': 'relators:evp',
            'Expert': 'relators:exp',
            'Facsimilist': 'relators:fac',
            'Field director': 'relators:fld',
            'Film director': 'relators:fmd',
            'Film distributor': 'relators:fds',
            'Film editor': 'relators:flm',
            'Film producer': 'relators:fmp',
            'Filmmaker': 'relators:fmk',
            'First party': 'relators:fpy',
            'Forger': 'relators:frg',
            'Former owner': 'relators:fmo',
            'Funder': 'relators:fnd',
            'Geographic information specialist': 'relators:gis',
            'Graphic technician (deprecated, use Artist)': 'relators:grt',
            'Honoree': 'relators:hnr',
            'Host': 'relators:hst',
            'Host institution': 'relators:his',
            'Illuminator': 'relators:ilu',
            'Illustrator': 'relators:ill',
            'Inscriber': 'relators:ins',
            'Instrumentalist': 'relators:itr',
            'Interviewee': 'relators:ive',
            'Interviewer': 'relators:ivr',
            'Inventor': 'relators:inv',
            'Issuing body': 'relators:isb',
            'Issuer': 'relators:isb',
            'Judge': 'relators:jud',
            'Jurisdiction governed': 'relators:jug',
            'Laboratory': 'relators:lbr',
            'Laboratory director': 'relators:ldr',
            'Landscape architect': 'relators:lsa',
            'Lead': 'relators:led',
            'Lender': 'relators:len',
            'Libelant': 'relators:lil',
            'Libelant-appellant': 'relators:lit',
            'Libelant-appellee': 'relators:lie',
            'Libelee': 'relators:lel',
            'Libelee-appellant': 'relators:let',
            'Libelee-appellee': 'relators:lee',
            'Librettist': 'relators:lbt',
            'Licensee': 'relators:lse',
            'Licensor': 'relators:lso',
            'Lighting designer': 'relators:lgd',
            'Lithographer': 'relators:ltg',
            'Lyricist': 'relators:lyr',
            'Manufacture place': 'relators:mfp',
            'Manufacturer': 'relators:mfr',
            'Marbler': 'relators:mrb',
            'Markup editor': 'relators:mrk',
            'Medium': 'relators:med',
            'Metadata contact': 'relators:mdc',
            'Metal-engraver': 'relators:mte',
            'Minute taker': 'relators:mtk',
            'Moderator': 'relators:mod',
            'Monitor': 'relators:mon',
            'Music copyist': 'relators:mcp',
            'Musical director': 'relators:msd',
            'Musician': 'relators:mus',
            'Narrator': 'relators:nrt',
            'Onscreen presenter': 'relators:osp',
            'Opponent': 'relators:opn',
            'Organizer': 'relators:orm',
            'Originator': 'relators:org',
            'Other': 'relators:oth',
            'Owner': 'relators:own',
            'Panelist': 'relators:pan',
            'Papermaker': 'relators:ppm',
            'Patent applicant': 'relators:pta',
            'Patent holder': 'relators:pth',
            'Patron': 'relators:pat',
            'Performer': 'relators:prf',
            'Permitting agency': 'relators:pma',
            'Photographer': 'relators:pht',
            'Plaintiff': 'relators:ptf',
            'Plaintiff-appellant': 'relators:ptt',
            'Plaintiff-appellee': 'relators:pte',
            'Platemaker': 'relators:plt',
            'Praeses': 'relators:pra',
            'Presenter': 'relators:pre',
            'Printer': 'relators:prt',
            'Printer of plates': 'relators:pop',
            'Printmaker': 'relators:prm',
            'Process contact': 'relators:prc',
            'Producer': 'relators:pro',
            'Production company': 'relators:prn',
            'Production designer': 'relators:prs',
            'Production manager': 'relators:pmn',
            'Production personnel': 'relators:prd',
            'Production place': 'relators:prp',
            'Programmer': 'relators:prg',
            'Project director': 'relators:pdr',
            'Proofreader': 'relators:pfr',
            'Provider': 'relators:prv',
            'Publishing director': 'relators:pbd',
            'Puppeteer': 'relators:ppt',
            'Radio director': 'relators:rdd',
            'Radio producer': 'relators:rpc',
            'Recording engineer': 'relators:rce',
            'Recordist': 'relators:rcd',
            'Redaktor': 'relators:red',
            'Renderer': 'relators:ren',
            'Reporter': 'relators:rpt',
            'Repository': 'relators:rps',
            'Research team head': 'relators:rth',
            'Research team member': 'relators:rtm',
            'Researcher': 'relators:res',
            'Respondent': 'relators:rsp',
            'Respondent-appellant': 'relators:rst',
            'Respondent-appellee': 'relators:rse',
            'Responsible party': 'relators:rpy',
            'Restager': 'relators:rsg',
            'Restorationist': 'relators:rsr',
            'Reviewer': 'relators:rev',
            'Rubricator': 'relators:rbr',
            'Scenarist': 'relators:sce',
            'Scientific advisor': 'relators:sad',
            'Screenwriter': 'relators:aus',
            'Scribe': 'relators:scr',
            'Sculptor': 'relators:scl',
            'Second party': 'relators:spy',
            'Secretary': 'relators:sec',
            'Seller': 'relators:sll',
            'Set designer': 'relators:std',
            'Setting': 'relators:stg',
            'Signer': 'relators:sgn',
            'Singer': 'relators:sng',
            'Sound designer': 'relators:sds',
            'Speaker': 'relators:spk',
            'Sponsor': 'relators:spn',
            'Stage director': 'relators:sgd',
            'Stage manager': 'relators:stm',
            'Standards body': 'relators:stn',
            'Stereotyper': 'relators:str',
            'Storyteller': 'relators:stl',
            'Supporting host': 'relators:sht',
            'Surveyor': 'relators:srv',
            'Teacher': 'relators:tch',
            'Technical director': 'relators:tcd',
            'Television director': 'relators:tld',
            'Television producer': 'relators:tlp',
            'Thesis advisor': 'relators:ths',
            'Transcriber': 'relators:trc',
            'Translator': 'relators:trl',
            'Type designer': 'relators:tyd',
            'Typographer': 'relators:tyg',
            'University place': 'relators:uvp',
            'Videographer': 'relators:vdg',
            'Vocalist (deprecated, use Singer)': 'relators:voc',
            'Voice actor': 'relators:vac',
            'Witness': 'relators:wit',
            'Wood engraver': 'relators:wde',
            'Woodcutter': 'relators:wdc',
            'Writer of accompanying material': 'relators:wam',
            'Writer of added commentary': 'relators:wac',
            'Writer of added lyrics': 'relators:wal',
            'Writer of added text': 'relators:wat',
            'Writer of introduction': 'relators:win',
            'Writer of preface': 'relators:wpr',
            'Writer of supplementary textual content': 'relators:wst'
        }
        self.fields = self.get_fields()
        self.to_harvest = [
                              'subject', 'titleInfo', 'originInfo', 'titleInfo', 'physicalDescription',
                              'typeOfResource', 'name', 'relatedItem'] + list(self.fields.keys())

    def get_fields(self):
        return {
            'note': 'field_abstract',
            'abstract': 'field_abstract',
            'dateIssued': 'field_edtf_date_issued',
            'publisher': 'field_publisher',
            'title': 'title',
            'genre': 'field_genre',
            'typeOfResource': 'field_resource_type',
            'accessCondition': 'field_access_condition',
            'dateOther': 'field_edtf_date_other',
            'dateCreated': 'field_edtf_date_created',
            'place': 'field_location',
            'copyrightDate': 'field_edtf_copyright_date',
            'issuance': 'field_issuance',
            'edition': 'field_edition',
            'identifier': 'field_identifier',
            'sub_title': 'field_subtitle'
        }

    def fix_dates(self, key):
        months = {
            "January": "01", "February": "02", "March": "03", "April": "04",
            "May": "05", "June": "06", "July": "07", "August": "08",
            "September": "09", "October": "10", "November": "11", "December": "12"
        }
        mistakes = {
            '8 Feb 1990': '1990-02-08',
            'Sept 1993': '1993-09',
            'Winter 2005': '2005-24',
            'November. 2008': '2008-11',
            'Between 1949 and 1965': '1949-1965',
            'Between 1953 and 1966': '1953-1966',
            '[before 1970]': '-1970'

        }
        date = self.summary[key].strip()
        if date is None:
            return
        if date in mistakes:
            date = mistakes[date]
        date = date.replace(';', '').replace(',', '')

        # Test for January 1973
        match = re.search(r'(\w+)\s+(\d{4})', date)
        if match:
            month_str, year = match.groups()
            month_map = {
                "January": "01", "February": "02", "March": "03",
                "April": "04", "May": "05", "June": "06",
                "July": "07", "August": "08", "September": "09",
                "October": "10", "November": "11", "December": "12"
            }
            month = month_map.get(month_str, "??")  # Handle unknown months gracefully
            self.summary[key] = f"{year}-{month}"
            return

        # Test for Jan 1999
        match = re.match(r"([A-Za-z]+),?\s*(\d{4})", date)
        if match:
            month_name, year = match.groups()
            month_number = months.get(month_name)  # Convert month name to number
            if month_number:
                return f"{year}-{month_number}"
        # Test for November-December, 2010
        match = re.match(r"([A-Za-z]+)-([A-Za-z]+),?\s*(\d{4})", date)
        if match:
            month1, month2, year = match.groups()
            month1_num = months.get(month1)
            month2_num = months.get(month2)
            if month1_num and month2_num:
                return f"{year}-{month1_num}/{year}-{month2_num}"

        # Test for 1982-83
        pattern = r"\b(18|19|20)\d{2}-(\d{2})\b"
        if re.match(pattern, date):
            years = date.split('-')
            century = years[0][:2]
            if years[0] == '1999':
                century = '20'
            self.summary[key] = f"{years[0]}/{century}{years[1]}"
            return
        # Test for February 27, 2010
        pattern = r"^(January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2},\s?\d{4}$"
        if re.match(pattern, date):
            try:
                if date == 'February 29, 1990':
                    date = 'February 28, 1990'
                date = re.sub(r",(\S+)", r", \1", date)
                date_object = datetime.strptime(date, "%B %d, %Y")
                self.summary[key] = date_object.strftime("%Y-%m-%d")
                return
            except ValueError as e:
                print(f"Error parsing date: '{date}'. Ensure it follows 'Month day, Year' format. ({e})")
                return
        pattern = r"\d{4}-\d{4}"
        if re.match(pattern, date):
            self.summary[key] = date.replace('-', '/')
            return
        if 'ca.' in date:
            self.summary[key] = f"{date.split()[-1]}~"
            return
        if edtf_validate.valid_edtf.is_valid(date):
            self.summary[key] = date
            return

        print(f"{date} could not be made EDTF compliant")

    def parse_name(self, input):
        role = name = ''
        outputs = []
        vocab = 'corporate_body'
        type_map = {
            'corporate': 'corporate_body',
            'personal': 'person'
        }
        for key, value in input.items():
            if key == '@type':
                vocab = type_map[value]
                continue
            if key == 'namePart':
                name = value
            if key == 'role' and not isinstance(value, str):
                if isinstance(value, list) and isinstance(value[0], dict):
                    value = value[0]
                else:
                    value = {}

                roleTerm = value.get('roleTerm', {})
                if isinstance(roleTerm, str):
                    role = roleTerm.capitalize()
                else:
                    role = value.get('roleTerm', {}).get('#text', '').capitalize()
        if role not in self.relator_map:
            role = 'Editor'
        return f"{self.relator_map[role]}:{vocab}:{name}"

    def extract_from_mods(self, mods):
        self.summary = {}
        result = xmltodict.parse(mods)
        mods = result['mods']
        string_keys = [key for key, value in mods.items() if isinstance(value, str) and not key.startswith("@")]
        all_keys = [key for key, value in mods.items() if not key.startswith("@")]
        ignored = [item for item in all_keys if item not in self.to_harvest]
        #   print(f"ignored- {ignored}")
        # Process simple string values.
        for key in string_keys:
            self.summary[self.fields[key]] = ' '.join(mods[key].splitlines())
            del (mods[key])

        # Origin info
        if isinstance(mods.get('originInfo'), dict):
            mods['originInfo'] = [mods['originInfo']]
        for originInfo in mods.get('originInfo', []):
            for key, value in originInfo.items():
                self.summary[self.fields[key]] = value

        # Subject info
        for key, value in mods.get('subject', {}).items():
            if key == 'geographic':
                self.summary['field_geographic_subject'] = value
            if key == 'topic':
                self.summary['field_subject'] = value
            if key == 'hierarchicalGeographic':
                location = {k: v for k, v in value.items() if v is not None}
                self.summary['field_geographic_subject'] = ','.join(location.values())

        # Related item
        if isinstance(mods.get('relatedItem'), dict):
            mods['relatedItem'] = [mods['relatedItem']]
        related_items = []
        for item in mods.get('relatedItem', {}):
            related_items.append(item.get('titleInfo', {}).get('title', ''))
        self.summary['field_related_item'] = '|'.join(related_items)

        # Title info
        if isinstance(mods['titleInfo'], dict):
            mods['titleInfo'] = [mods['titleInfo']]
        for title in mods['titleInfo']:
            if 'title' not in self.summary and title.get('title') is not None:
                self.summary['title'] = title.get('title')
            if 'field_subtitle' not in self.summary and title.get('field_subtitle'):
                self.summary['field_subtitle'] = title.get('subtitle', '')
            if title.get('@type', '') == 'alternative':
                if title.get('title') is not None:
                    self.summary['field_alternative_title'] = title.get('title')
        # Location.
        if isinstance(mods.get('location'), dict):
            mods['location'] = [mods['location']]
        locations = []
        for location in mods.get('location', {}):
            locations.append((location or {}).get('location', {}).get('physicalLocation', ''))
            self.summary['field_location'] = '|'.join(locations)
        self.summary['field_physical_description'] = (
            mods.get('physicalDescription', {})
            .get('form', {})
            .get('#text', '')
        )
        self.summary['field_extent'] = mods.get('physicalDescription', {}).get('extent', '')
        self.summary['field_resource_type'] = (mods.get('typeOfResource') or {}).get('#text', '')
        if isinstance(mods.get('name'), dict):
            mods['name'] = [mods['name']]
        names = []
        for name in mods.get('name', []):
            names.append(self.parse_name(name))
        self.summary['field_linked_agent'] = '|'.join(names)

        for key in self.summary:
            if 'edtf' in key:
                self.fix_dates(key)

        return self.summary
