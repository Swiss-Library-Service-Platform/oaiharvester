#################
# OAI Harvester #
#################

# This module is used to harvest data from an OAI server

import requests
from lxml import etree
from typing import Optional, Iterable
import logging
from datetime import date
import time
import os
from .tools import ns, check_free_space
from .records import XmlRecord


class OaiSet:
    """
    Class representing an OAI set

    Attributes
    ----------
    set_name : str
        name of the set to harvest
    chunk_paths : list of str
        list of chunks of harvested records
    base_url: str
        url of the oai server
    error : bool
        True if an error occurred during the harvesting
    error_messages : List[str]
        Empty list if no error, error message in case of error
    """

    def __init__(self, set_name: str, base_url: str) -> None:
        """
        Construct OAI set object

        parameters
        ----------
        set_name : str
            name of the set to harvest
        base_url : str
            url to use to harvest oai, if not provided use the constant value BASE_URL

        """
        self.error = False
        self.error_messages = []
        self.set_name = set_name
        self.base_url = base_url
        self.chunk_paths = []
        self._harvest_directory = None

    def __repr__(self) -> str:
        """
        Used to pretty display records

        returns
        -------
        Set name
        """
        return f'OaiSet_{self.set_name}'

    def get_harvest_directory(self) -> str:
        """
        Get the folder path to save chunk files

        If the folder doesn't exist, it will be created
        """
        if self._harvest_directory is not None:
            return self._harvest_directory

        directory_path = f'harvested_data/OaiSet_{self.set_name}_{date.today().strftime("%Y%m%d")}'
        if not os.path.isdir('harvested_data'):
            os.mkdir('harvested_data')

        if not os.path.isdir(directory_path):
            os.mkdir(directory_path)

        self._harvest_directory = directory_path

        return self._harvest_directory

    def get_next_chunk_path(self) -> str:
        """
        Save chunks into a folder according to the date and set name

        Parameter
        ---------
        chunk : Chunk
            chunk to save
        """
        return f'{self.get_harvest_directory()}/{self.get_next_chunk_file_name()}'

    def get_next_chunk_file_name(self) -> str:
        """
        Get the file name of the next chunk without the extension

        Returns
        -------
        str : file name of the chunk
        """
        nb_harvested_chunks = len(os.listdir(self.get_harvest_directory()))
        return f'chunk_{self.set_name}_{str(nb_harvested_chunks+1).zfill(5)}.xml'

    def get_next_chunk(self, from_time: Optional[str] = None, to_time: Optional[str] = None) -> Iterable['Chunk']:
        """
        Iterator: get the next chunk of records

        Returns
        -------
        Chunk : next chunk
        """
        logging.info(f'{repr(self)}: Start harvesting set "{self.set_name}" - base url: {self.base_url}')

        if os.path.isdir(self.get_harvest_directory()):
            for file in os.listdir(self.get_harvest_directory()):
                os.remove(f'{self.get_harvest_directory()}/{file}')
            logging.warning(f'{repr(self)}: harvest directory cleaned')

        resumption_token = None

        while True:
            chunk = Chunk(oai_set=self, from_time=from_time, to_time=to_time, resumption_token=resumption_token)
            if ('Impossible to fetch chunk data' in chunk.error_messages or
                    'Not enough free space to save the chunk' in chunk.error_messages):
                logging.critical(f'{repr(self)}: Impossible to fetch chunk data, stopping harvesting')
                self.error = True
                self.error_messages.append('Impossible to fetch chunk data')
                break

            resumption_token = chunk.get_resumption_token()
            if resumption_token is None:
                logging.info(f'{repr(chunk)}: no resumption token')
                yield chunk
                break
            logging.info(f'{repr(chunk)}: {resumption_token}')
            yield chunk


class Chunk:
    """
    Class attribute
    ---------------
    parser : etree.XMLParser
        parser used to be able to pretty print harvested records

    Attributes
    ----------
    oai_set : OaiSet
        the OaiSet object to harvest
    error : bool
        True if an error occurred during the harvesting
    error_messages : List[str]
        None if no error, error message in case of error
    path : str
        path to file where the chunk is saved
    content : str
        xml data containing the records and the response of the OAI server
    xml : etree.Element, optional
        xml data containing the records if etree.Element, return None if an error occurred

    """

    parser = etree.XMLParser(remove_blank_text=True)

    def __init__(self,
                 oai_set: OaiSet,
                 from_time: Optional[str] = None,
                 to_time: Optional[str] = None,
                 resumption_token: Optional[str] = None,
                 file_path=None) -> None:
        """
        Construct `Chunk` object

        Parameters
        ----------
        oai_set : OaiSet
            The OaiSet object to harvest
        resumption_token : str
            First chunk of OAI don't need resumption token to be harvested. But resumption token is used to
            fetch next chunks
        from_time : str
            Used for incremental harvesting. This decides which records will be harvested.
        to_time : str
            Used for incremental harvesting. This decides which records will be harvested.
        file_path : str
            Path to file load data instead of fetching it
        """
        self.oai_set = oai_set
        self.error = False
        self.error_messages = []

        if file_path is not None:
            self.path = file_path
            self.content = self.fetch_data_from_file(file_path)
        else:
            self.path = self.oai_set.get_next_chunk_path()
            self.content = self.fetch_data(from_time=from_time, to_time=to_time, resumption_token=resumption_token)

        self.xml = self.parse_xml()

        self.records = list(self.get_records())

        if len(self.records) == 0:
            self.error_messages.append('No records in chunk')
            self.error = True
            logging.error(f'{repr(self)}: no records in chunk')

            if self.xml is not None and self.xml.find('error_message') is not None:
                self.error_messages.append(self.xml.find('error_message').text)
                logging.error(f'{repr(self)}: {self.xml.find("error_message").text}')

    def __str__(self) -> str:
        """
        Used to pretty print the record

        Returns
        -------
        str : xml data as text
        """
        if self.xml is not None:
            return etree.tostring(self.xml, pretty_print=True).decode('utf-8', errors='ignore')
        elif self.content is not None:
            return self.content.decode('utf-8', errors='ignore')
        else:
            return ''

    def __repr__(self) -> str:
        """
        Used to describe chunk in logs

        returns
        -------
        str : pattern to display chunk in logs
        """
        return f'{self.path.split("/")[-1]}'

    def get_resumption_token(self) -> Optional[str]:
        """Fetch resumption token

        Resumption token is used to fetch the next chunks. If no resumption token is
        available in the data, that means that we got the last chunk.

        Returns
        -------
        str : resumption token
        """
        if self.xml is not None:
            token = self.xml.find(".//{http://www.openarchives.org/OAI/2.0/}resumptionToken")
            if token is not None and token.text:
                logging.info(f'{repr(self)}: resumption token found: {token.text}')
                return token.text

        if self.error is True and 'Badly formatted xml data' in self.error_messages and self.content is not None:
            xml_txt = self.content.decode(encoding='utf-8', errors='ignore')
            token_begin = xml_txt.rfind('<resumptionToken>') + len('<resumptionToken>')
            token_end = xml_txt.rfind('</resumptionToken>')
            token = xml_txt[token_begin:token_end]
            logging.info(f'{repr(self)}: resumption token found: {token}')
            return token

        logging.warning(f'{repr(self)}: no resumption token')
        return None

    def fetch_data(self, resumption_token: str = None, from_time: str = None, to_time: str = None) -> Optional[bytes]:
        """
        Fetch data using http request

        Parameter
        ---------
        str : resumption token
            First chunk of OAI don't need resumption token to be harvested. But resumption token is used to
            fetch next chunks

        Returns
        -------
        bytes : data fetched from the OAI server, return None if an error occurred

        """

        # Build parameters of the http request
        if resumption_token is None:
            params = {'verb': 'ListRecords',
                      'metadataPrefix': 'marc21',
                      'set': self.oai_set.set_name,
                      'from': from_time,
                      'until': to_time}
        else:
            params = {'verb': 'ListRecords',
                      'resumptionToken': resumption_token}

        nb_tries = 0
        r = None
        while nb_tries < 3:
            nb_tries += 1
            try:
                r = requests.get(self.oai_set.base_url, params=params)
                if r.ok is True:
                    logging.info(f'{repr(self)}: data fetched - {r.url}')
                    break
            except requests.exceptions.RequestException as e:
                logging.error(f'{repr(self)}: error when fetching data - try {nb_tries} - {e}')
                time.sleep(5 * nb_tries)
                r = None

        if r is not None and r.ok is False:
            logging.error(f'{repr(self)} - impossible to fetch data - {r.url} - '
                          f'{r.status_code} - {r.content.decode("utf-8")}')
            self.error_messages.append('Impossible to fetch chunk data')
            self.error = True
            return None

        if r is None:
            logging.error(f'{repr(self)}: impossible to fetch data')
            self.error_messages.append('Impossible to fetch chunk data')
            return None

        return r.content

    def fetch_data_from_file(self, file_path: str) -> Optional[bytes]:
        """
        Fetch data from file

        Parameter
        ---------
        file_path : str
            path to file to load data from

        Returns
        -------
        bytes : data from the file, return None if an error occurred
        """
        try:
            with open(file_path, 'rb') as f:
                logging.info(f'{repr(self)}: fetching data from file - {file_path}')
                return f.read()

        except FileNotFoundError:
            logging.error(f'{repr(self)}: file not found - {file_path}')
            self.error_messages.append(f'File not found - {file_path}')
            self.error = True
            return None

    def save(self) -> None:
        """
        Save chunks into a folder according to the date and set name

        Parameter
        ---------
        file_name : str
            path to file to save the current chunk

        """
        if check_free_space(self.oai_set.get_harvest_directory(), low_limit=25, error_limit=10) is False:
            logging.error(f'{repr(self)}: not enough free space to save the chunk')
            self.error_messages.append('Not enough free space to save the chunk')
            self.error = True
            return
        with open(self.path, 'w') as f:
            f.write(str(self))

        logging.info(f'{repr(self)} saved')

    def delete(self) -> None:
        """
        Delete the chunk file from the disk
        """
        try:
            os.remove(self.path)
            logging.info(f'{repr(self)}: chunk deleted')
        except FileNotFoundError:
            logging.warning(f'{repr(self)}: chunk file not found')

    def parse_xml(self) -> Optional[etree.Element]:
        """
        Parse xml data

        Returns
        -------
        etree.Element : xml data containing the records and the response of the OAI server, return
        None if an error occurred
        """
        if self.content is None:
            return None
        try:
            return etree.fromstring(self.content, parser=self.parser)
        except etree.XMLSyntaxError as e:
            self.error = True
            self.error_messages.append('Badly formatted xml data')
            logging.error(f'{repr(self)}: Error when parsing xml data - {e}')
        return None

    def get_records(self) -> list:
        """
        Get records from the chunk

        Returns
        -------
        list : list of etree.Element
        """
        if self.xml is not None:
            return [XmlRecord(xml_data=record_data) for record_data
                    in self.xml.findall("oai:ListRecords/oai:record", namespaces=ns)]

        if self.content is not None:
            xml_txt = self.content.decode(encoding='utf-8', errors='ignore')
            new_record_tag = ('<record xmlns="http://www.openarchives.org/OAI/2.0/" '
                              'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">')
            records = xml_txt.split('<record>')
            if len(records) > 1:
                records = [XmlRecord(xml_data=new_record_tag + record[:record.rfind('</record>') + len('</record>')])
                           for record in records[1:] if '</record>' in record]

                return records
        return []
