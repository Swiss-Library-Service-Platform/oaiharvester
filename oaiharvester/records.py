from abc import ABC
from typing import Union, Optional
from lxml import etree
import json
# from .tools import ns
import re
import logging
from datetime import datetime
from copy import deepcopy

ns = {'oai': 'http://www.openarchives.org/OAI/2.0/',
      'marc': 'http://www.loc.gov/MARC21/slim'}

class Record(ABC):
    """
    Abstract class representing a record

    Attributes
    ----------
    error : bool
        Error flag
    error_messages : list
        List of error messages
    mms_id : str
        MMS ID of the record
    """

    def __init__(self) -> None:
        """Initialize Record Object
        """
        self.error = False
        self.data_error = False
        self.error_messages = []
        self.data_error_messages = []
        self.mms_id = None

    def __repr__(self):
        """Representation of Record object
        """
        if self.mms_id is not None:
            return f'{self.__class__.__name__}(<{self.mms_id}>)'
        else:
            return f'{self.__class__.__name__}(<unknown mms_id>)'

    def clean_f_indicator(self, indicator: Optional[str], tag: Optional[str]) -> str:
        """
        Clean the indicator

        Parameters
        ----------
        indicator : str
            Indicator to clean
        tag : str
            Tag of the field

        Returns
        -------
        str
            Cleaned indicator
        """
        if indicator is None:
            self.data_error = True
            self.data_error_messages.append(f'{repr(self)}: indicator is None (tag {tag})')
            logging.error(f'{repr(self)}: indicator is  (tag {tag})')
            return 'ERROR'
        if re.match(r'^[a-zA-Z0-9\s]$', indicator) is None:
            self.data_error = True
            self.data_error_messages.append(f'{repr(self)}: invalid indicator: {indicator} (tag {tag})')
            logging.error(f'{repr(self)}: invalid indicator: {indicator} (tag {tag})')
            return 'ERROR'
        return indicator

    def clean_f_tag(self, tag: Optional[str]) -> str:
        """
        Clean the tag

        Parameters
        ----------
        tag : str
            Tag to clean

        Returns
        -------
        str
            Cleaned tag
        """
        if tag is None:
            self.data_error = True
            self.data_error_messages.append(f'{repr(self)}: Tag is None')
            logging.error(f'{repr(self)}: tag is None')
            return 'ERROR'
        if re.match(r'^[0-9]{3}$', tag) is None:
            self.data_error = True
            self.data_error_messages.append(f'{repr(self)}: invalid tag: {tag}')
            logging.error(f'{repr(self)}: invalid tag: {tag}')
            return 'ERROR'
        return tag

    def clean_subf_code(self, code: Optional[str], tag: Optional[str]) -> str:
        """
        Clean the code of subfields

        It will add 'n' to the code if it is a number. In case of invalid code,
        it will return 'ERROR'

        Parameters
        ----------
        code : str
            Indicator to clean
        tag : str
            Tag of the field

        Returns
        -------
        str
            Cleaned indicator
        """
        # Check if code is None
        if code is None:
            self.data_error = True
            self.data_error_messages.append(f'{repr(self)}: Code is None')
            logging.error(f'{repr(self)}: code is None')
            return 'ERROR'

        # Check if the code is a single character, exception for 'n' followed by a number
        if re.match(r'^(?:[a-zA-Z0-9]|n\d)$', code) is None:
            self.data_error = True
            self.data_error_messages.append(f'{repr(self)}: invalid code: {code} (tag {tag})')
            logging.error(f'{repr(self)}: invalid code: {code} (tag {tag})')
            return 'ERROR'

        # Add 'n' to the code if it is a number
        if re.match(r'^\d$', code) is not None:
            return 'n' + code
        return code


class XmlRecord(Record):
    """
    Class representing an xml record

    Attributes
    ----------
    data : etree.Element
        XML data
    mms_id : str
        MMS ID of the record

    """

    def __init__(self, xml_data: Union[str, bytes, etree.Element]) -> None:
        super().__init__()
        self.data = self._get_valid_xml_data(xml_data)
        self.mms_id = self._get_mms_id(xml_data)
        if self.error is True:
            logging.error(f'{repr(self)}: {" / ".join(self.error_messages)}')

    def __str__(self) -> str:
        """
        String representation of data

        Returns
        -------
        str
            String representation of data
        """
        if self.data is None:
            return ''
        else:
            return etree.tostring(self.data, pretty_print=True).decode('utf-8', errors='ignore')

    def _get_valid_xml_data(self, xml_data: Union[str, bytes, etree.Element]) -> etree.Element:
        """Get valid xml data
        """
        if isinstance(xml_data, str):
            try:
                return etree.fromstring(xml_data.encode('utf-8', errors='ignore'))
            except etree.XMLSyntaxError as e:
                self.error = True
                self.error_messages.append(f'Invalid xml data: {e}')
                return None
        elif isinstance(xml_data, bytes):
            try:
                return etree.fromstring(xml_data)
            except etree.XMLSyntaxError as e:
                self.error = True
                self.error_messages.append(f'Invalid xml data: {e}')
                return None
        elif isinstance(xml_data, etree._Element):
            return xml_data
        else:
            self.error = True
            self.error_messages.append(f'Invalid xml data type: {type(xml_data)}')
            return None

    def _get_mms_id(self, xml_data) -> Optional[str]:
        """
        Get MMS ID

        Parameters
        ----------
        xml_data : Union[str, bytes, etree.Element]
            XML data

        Returns
        -------
        str
            MMS ID of the record
        """
        if self.data is not None:
            if self.is_deleted() is True:
                identifier = self.data.find('oai:header/oai:identifier', namespaces=ns)
                if identifier.text is not None:
                    m = re.search(r'(?<=:)(\d+)$', identifier.text)
                    if m is not None:
                        return m.group(1)
                self.error = True
                self.error_messages.append('MMS ID not found')
                return None

            mms_id = self.data.find('oai:metadata/marc:record/marc:controlfield[@tag="001"]', namespaces=ns)
            if mms_id is not None:
                return mms_id.text
            else:
                self.error = True
                self.error_messages.append('MMS ID not found')
                return None

        if isinstance(xml_data, bytes):
            xml_data = xml_data.decode('utf-8', errors='ignore')

        if not isinstance(xml_data, str):
            self.error = True
            self.error_messages.append('MMS ID not found')
            return None

        m = re.search(r'<controlfield tag="001">(99\d+)</controlfield>', xml_data)
        if m is not None:
            return m.group(1)
        else:
            self.error = True
            self.error_messages.append('MMS ID not found')
            return None

    def to_json(self) -> Optional['JsonRecord']:
        """
        Convert to JSON record for MongoDB

        Returns
        -------
        JsonRecord
            JSON record
        """
        if self.data is None or self.mms_id is None:
            return None

        json_record = {'mms_id': self.mms_id,
                       'marc': dict()}

        datestamp = self.data.find('oai:header/oai:datestamp', namespaces=ns)
        if datestamp is not None:
            json_record['p_date'] = datetime.strptime(datestamp.text, '%Y-%m-%dT%H:%M:%SZ')
        else:
            self.error = True
            self.error_messages.append(f'{repr(self)}: Datestamp not found')
            logging.error(f'{repr(self)}: datestamp not found')
            return None

        # No metadata in case of deleted record
        if self.is_deleted() is True:
            json_record['deleted'] = True
            return JsonRecord(json_record, data_error_messages=self.data_error_messages)

        xml_metadata = self.data.find('oai:metadata/marc:record', ns)
        if xml_metadata is None:
            self.error = True
            self.error_messages.append(f'{repr(self)}: Metadata not found')
            logging.error(f'{repr(self)}: metadata not found')
            return None

        # Get leader field
        leader = xml_metadata.find('marc:leader', ns)
        if leader is not None:
            json_record['marc']['leader'] = leader.text
        else:
            self.error = True
            self.error_messages.append(f'{repr(self)}: Leader field not found')
            logging.error(f'{repr(self)}: leader field not found')
            return None

        # Extract data from controlfields
        for controlfield in xml_metadata.findall('marc:controlfield', ns):
            tag = self.clean_f_tag(controlfield.get('tag'))

            json_record['marc'][tag] = controlfield.text

        # Extract data from datafields
        for datafield in xml_metadata.findall('marc:datafield', ns):
            tag = self.clean_f_tag(datafield.get('tag'))

            # Tag 988 is used to store administrative data
            if tag == '988':
                json_record['sup'] = datafield.find('marc:subfield[@code="e"]', ns).text == 'true'

                c_date = datafield.find('marc:subfield[@code="b"]', ns)
                if c_date is not None and c_date.text is not None:
                    m = re.match(r'([\d\-:\s]+)\s\D', c_date.text)
                    if m is not None:
                        json_record['c_date'] = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S')

                u_date = datafield.find('marc:subfield[@code="d"]', ns)
                if u_date is not None and u_date.text is not None:
                    m = re.match(r'([\d\-:\s]+)\s\D', u_date.text)
                    if m is not None:
                        json_record['u_date'] = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S')
                continue

            datafield_data = dict()

            datafield_data['ind1'] = self.clean_f_indicator(datafield.get('ind1'), tag)
            datafield_data['ind2'] = self.clean_f_indicator(datafield.get('ind2'), tag)
            subfields = datafield.findall('marc:subfield', ns)
            datafield_data['sub'] = [{self.clean_subf_code(subfield.get('code'), tag): subfield.text} for subfield in
                                     subfields if subfield.text is not None]
            if tag not in json_record['marc']:
                json_record['marc'][tag] = list()

            json_record['marc'][tag].append(datafield_data)

        return JsonRecord(json_record, data_error_messages=deepcopy(self.data_error_messages))

    def is_deleted(self) -> bool:
        """
        Check if record is deleted

        Returns
        -------
        bool
            True if record is deleted, False otherwise
        """
        if self.data is None:
            return False

        header = self.data.find('oai:header', namespaces=ns)
        if header is not None:
            if header.get('status') == 'deleted':
                return True

        return False


class JsonRecord(Record):
    """
    Class representing a JSON record

    Attributes
    ----------
    data : dict
        JSON data
    data_error : bool
        Error flag on the data
    data_error_messages : list
        List of error messages on the data
    mms_id : str
        MMS ID of the record
    error : bool
        Error flag (record data will be unavailable if True)
    error_messages : list
        List of error messages
    """

    def __init__(self, json_data: dict, data_error_messages: Optional[list] = None) -> None:
        """
        Initialize JsonRecord object

        Parameters
        ----------
        json_data : dict
            JSON data
        """
        super().__init__()
        if json_data is None:
            self.error = True
            self.error_messages.append('JSON data is None')
            return

        self.data = json_data
        self.mms_id = self.data.get('mms_id')
        self.p_date = self.data.get('p_date')
        self.deleted = self.data.get('deleted', False)

        # History record don't have "marc" field
        if self.data.get('marc') is not None:
            if self.deleted is False:
                self.data['format'] = self.get_bib_resource_type()
            if self.deleted is False:
                self.data['access'] = self.get_access_type()
            self.format = self.data.get('format')
            self.access = self.data.get('access')

        if data_error_messages is not None and len(data_error_messages) > 0:
            self.data_error = True
            self.data_error_messages += data_error_messages
            self.data['data_error'] = True
            self.data['data_error_messages'] = self.data_error_messages
        else:
            self.data_error = self.data.get('data_error', False)
            self.data_error_messages = self.data.get('data_error_messages', [])

    def __eq__(self, other: 'JsonRecord') -> bool:
        """
        Compare two JsonRecord objects

        Parameters
        ----------
        other : JsonRecord
            Other JsonRecord object

        Returns
        -------
        bool
            True if objects are equal, False otherwise
        """
        if self.mms_id == other.mms_id and self.p_date == other.p_date:
            return True
        return False

    def __hash__(self) -> int:
        """
        Return hash of the object

        This value is based on the mms_id and datestamp

        Returns
        -------
        int
            Hash of the object
        """
        return hash((self.mms_id, self.p_date))

    def __repr__(self) -> str:
        """
        Representation of JsonRecord object

        Returns
        -------
        str
            Representation of JsonRecord object
        """
        if self.mms_id is not None and self.p_date is not None:
            return f'{self.__class__.__name__}(<{self.mms_id} - {self.p_date}>)'
        else:
            return f'{self.__class__.__name__}(<unknown mms_id>)'

    def __str__(self) -> str:
        """
        String representation of data

        Returns
        -------
        str
            String representation of data
        """
        pass
        if self.data is None:
            return ''
        else:
            return json.dumps(self.data, indent=2)


    def get_bib_resource_type(self) -> str:
        """Get the resource type of the record

        The analysis is mainly based on the leader position 6 and 7.
        To distinguish between series and journal, we use the field
        008 pos. 6.

        Returns
        -------
        str
            Resource type of the record
        """

        if self.data['marc'].get('leader') is None or len(self.data['marc'].get('leader')) < 10:
            self.data_error = True
            self.data['data_error'] = True
            self.data_error_messages.append('Leader field not found')
            self.data['data_error_messages'] = self.data_error_messages
            return 'ERROR'

        if self.data['marc'].get('008') is None or len(self.data['marc'].get('008')) < 22:
            self.data_error = True
            self.data['data_error'] = True
            self.data_error_messages.append('008 field not found')
            self.data['data_error_messages'] = self.data_error_messages
            return 'ERROR'

        pos6 = self.data['marc']['leader'][6]
        pos7 = self.data['marc']['leader'][7]
        cf008 = self.data['marc'].get('008', None)
        if pos6 in 'a':
            if pos7 in 'acdm':
                return 'Book'
            elif pos7 in 'bis':
                if cf008[21] in 'pn':
                    return 'Journal'
                else:
                    return 'Series'

        elif pos6 in 'c':
            return 'Notated Music'

        elif pos6 in 'ij':
            return 'Audio'

        elif pos6 in 'ef':
            return 'Map'

        elif pos6 in 'dt':
            return 'Manuscript'

        elif pos6 in 'ef':
            return 'Map'

        elif pos6 in 'k':
            return 'Image'

        elif pos6 in 'ro':
            return 'Object'

        elif pos6 in 'g':
            return 'Video'

        elif pos6 in 'p':
            return 'Mixed Material'

        return 'Other'

    def get_access_type(self) -> Optional[str]:
        """Get the access type of the record

        Returns
        -------
        str
            Access type of the record
        """

        if self.data['marc'].get('leader') is None or len(self.data['marc'].get('leader')) < 10:
            self.data_error = True
            self.data['data_error'] = True
            if 'Leader field not found' not in self.data_error_messages:
                self.data_error_messages.append('Leader field not found')
            self.data['data_error_messages'] = self.data_error_messages
            return 'ERROR'

        if self.data['marc'].get('008') is None or len(self.data['marc'].get('008')) < 23:
            self.data_error = True
            self.data['data_error'] = True
            if '008 field not found' not in self.data_error_messages:
                self.data_error_messages.append('008 field not found')
            self.data['data_error_messages'] = self.data_error_messages
            return 'ERROR'

        if self.is_micro() is True:
            return 'M'
        if self.is_online() is True:
            return 'O'
        if self.is_braille() is True:
            return 'B'

        return 'P'


    def is_online(self) -> bool:
        """
        Check if the record is an online record.

        Use field 008 and leader. Position 23 indicate if a record is online or not (values "o",
         "q", "s"). For visual material and maps it's 29 position.

        Returns
        -------
        bool
            True if record is online, False otherwise
        """
        f338s = self.data['marc'].get('338', [])
        for datafield in f338s:
            for subfield in datafield['sub']:
                if subfield.get('b') == 'cr':
                    return True

        leader6 = self.data['marc']['leader'][6]
        f008 = self.data['marc']['008']
        format_pos = 29 if leader6 in 'egkor' else 23

        if len(f008) > format_pos:
            return f008[format_pos] in 'oqs'
        return False

    def is_micro(self):
        """Check if the record is a microform.

        Use field 008 and leader. Position 23 indicate if a record is online or not (values "a",
         "b", "c"). For visual material and maps it's 29 position.

        Returns
        -------
        bool
            True if record is a microform, False otherwise
        """
        leader6 = self.data['marc']['leader'][6]
        f008 = self.data['marc']['008']
        format_pos = 29 if leader6 in 'egkor' else 23

        f338s = self.data['marc'].get('338', [])
        for datafield in f338s:
            for subfield in datafield['sub']:
                if subfield.get('b', '').startswith('h'):
                    return True

        if len(f008) > format_pos:
            return f008[format_pos] in 'abc'
        return False

    def is_braille(self):
        """Check if the record is a Braille document.

        Use field 008 and leader. Position 23 indicate if a record is a Braille document or not
        (values "f"). For visual material and maps it's 29 position.

        Returns
        -------
        bool
            True if record is a Braille document, False otherwise
        """
        leader6 = self.data['marc']['leader'][6]
        f008 = self.data['marc']['008']
        format_pos = 29 if leader6 in 'egkor' else 23
        f336s = self.data['marc'].get('336', [])
        for datafield in f336s:
            for subfield in datafield['sub']:
                if subfield.get('b', '').startswith('tct'):
                    return True

        if len(f008) > format_pos:
            return f008[format_pos] in 'f'
        return False

    def to_archive(self) -> 'ArchiveJsonRecord':
        """
        Convert to archive record

        Returns
        -------
        JsonRecord
            Archive record
        """
        data = {'mms_id': self.data['mms_id'], 'p_date': self.data['p_date'],
                'c_date': deepcopy(self.data['c_date']), 'u_date': deepcopy(self.data['u_date']),
                'sup': self.data['sup'], 'deleted': self.deleted, 'versions': [deepcopy(self.data)]}
        if self.data_error is True:
            data['data_error'] = self.data_error
            data['data_error_messages'] = self.data_error_messages

        # Remove _id field of the MongoDB record
        data['versions'][0].pop('_id', None)

        return ArchiveJsonRecord(data)


class ArchiveJsonRecord(JsonRecord):
    """
    Class representing an archive record
    """

    def __init__(self, json_data: dict) -> None:
        """
        Initialize ArchiveJsonRecord object

        Parameters
        ----------
        json_data : dict
            JSON data
        """
        super().__init__(json_data)

    def add_record_to_archive(self, record: JsonRecord) -> 'ArchiveJsonRecord':
        """
        Add record to archive

        Parameters
        ----------
        record : JsonRecord
            Record to add to archive
        """
        if record.p_date in self.get_versions_p_date():
            logging.warning(f'{repr(record)}: already exists in the archive with this datestamp')
            return self
        data = deepcopy(record.data)

        if '_id' in data:
            del data['_id']

        self.data['versions'].append(data)
        self.sort_versions()
        self.filter_versions()
        try:
            self.data['u_date'] = self.data['versions'][-1]['u_date']
            self.data['c_date'] = self.data['versions'][-1]['c_date']
            self.data['p_date'] = self.data['versions'][-1]['p_date']
            self.data['sup'] = self.data['versions'][-1]['sup']
        except KeyError:
            self.data['data_error'] = True
            self.data['data_error_messages'] = f'{repr(record)}: corrupted record added to archive'

        if self.data['versions'][-1].get('data_error', False) is True:
            self.data['data_error'] = True
            self.data['data_error_messages'] = self.data['versions'][-1]['data_error_messages']
        elif 'data_error' in self.data:
            del self.data['data_error']
            if 'data_error_messages' in self.data:
                del self.data['data_error_messages']

        logging.info(f'{repr(record)}: added to the archive')
        return self

    def sort_versions(self) -> None:
        """
        Sort versions by date
        """
        self.data['versions'] = sorted(self.data['versions'], key=lambda x: x['p_date'])

    def get_versions_p_date(self) -> list:
        """
        Get versions datestamp

        Returns
        -------
        list
            List of datestamps
        """
        return [version['p_date'] for version in self.data['versions']]

    def filter_versions(self) -> None:
        """
        Filter versions by date. We keep the first, the last and the 3 versions.
        Additionally, we keep the first version of each year.
        """
        # We keep at least 4 versions
        if len(self.data['versions']) <= 4:
            return

        years = set()
        new_versions = []

        for i, v in enumerate(self.data['versions']):

            # Keep the first, the last and the 3 versions and the first version of each year
            if i == 0 or i >= len(self.data['versions']) - 3 or v['p_date'].year not in years:
                new_versions.append(v)
                years.add(v['p_date'].year)

        self.data['versions'] = new_versions
