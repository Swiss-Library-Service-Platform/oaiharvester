import time
import unittest
import oaiharvester.harvester as harvester
from oaiharvester.mongodb import Mongo
import oaiharvester.tools as tools
import shutil
import os
from lxml import etree
import configparser
from datetime import datetime, timedelta
import dotenv
import json

from records import JsonRecord, ArchiveJsonRecord

os.chdir(os.path.dirname(__file__))

dotenv.load_dotenv()

config = configparser.ConfigParser()
config.read('../config_test.cfg')
config.read('config_test.cfg')

base_url = config['OAI_harvesting']['BASE_URL']
db_name = config['MongoDB']['DB_NAME']
active_col = config['MongoDB']['ACTIVE_COL']
hist_col = config['MongoDB']['HIST_COL']
task_col = config['MongoDB']['TASK_COL']


class TestOaiHarvester(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        for d in os.listdir('./harvested_data'):
            if d != 'test':
                shutil.rmtree(f'./harvested_data/{d}')

    def test_OaiSet(self):
        oai_set = harvester.OaiSet(set_name='slsp_mongodb', base_url=base_url)
        oai_set.get_next_chunk_path()
        self.assertTrue(os.path.exists(oai_set.get_harvest_directory()))
        chunk = None
        for i, chunk in enumerate(oai_set.get_next_chunk()):
            if chunk is not None:
                chunk.save()
                self.assertTrue(os.path.exists(chunk.path))
            if i >= 2:
                break
        self.assertEqual(len(os.listdir(oai_set.get_harvest_directory())),
                         i + 1,
                         f'Number of chunks saved should {i + 1}')

        self.assertGreaterEqual(len(chunk.get_records()), 900, 'Chunk should have 900 or more records')

    def test_bad_Chunk_1(self):
        oai_set = harvester.OaiSet(set_name='slsp_mongodb', base_url=base_url)
        chunk = harvester.Chunk(oai_set=oai_set,
                                file_path='harvested_data/test/bad_chunk_1.xml')
        token = chunk.get_resumption_token()

        self.assertEqual(token, 'all@all@slsp_mongodb@marc21@67968877080005501', 'Token should be "all@all@slsp_mongodb@marc21@67968877080005501"')

    def test_bad_Chunk_2(self):
        oai_set = harvester.OaiSet(set_name='slsp_mongodb', base_url=base_url)
        chunk = harvester.Chunk(oai_set=oai_set,
                                file_path='harvested_data/test/bad_chunk_2.xml')
        token = chunk.get_resumption_token()

        self.assertEqual(token, 'all@all@slsp_mongodb@marc21@67968877080005501', 'Token should be "all@all@slsp_mongodb@marc21@67968877080005501"')

        records = [record for record in chunk.get_records() if record.error is False]

        self.assertEqual(len(records), 2, 'Must be 2 good records')

    def test_bad_Chunk_3(self):
        oai_set = harvester.OaiSet(set_name='slsp_mongodb', base_url=base_url)
        chunk = harvester.Chunk(oai_set=oai_set,
                                file_path='harvested_data/test/bad_chunk_3.xml')
        token = chunk.get_resumption_token()

        self.assertEqual(token, 'all@all@slsp_mongodb@marc21@67968877080005501', 'Token should be "all@all@slsp_mongodb@marc21@67968877080005501"')

    def test_bad_Chunk_4(self):
        oai_set = harvester.OaiSet(set_name='slsp_mongodb', base_url=base_url)
        chunk = harvester.Chunk(oai_set=oai_set,
                                file_path='harvested_data/test/bad_chunk_4.xml')
        token = chunk.get_resumption_token()

        self.assertEqual(token, 'all@all@slsp_mongodb@marc21@67968877080005501', 'Token should be "all@all@slsp_mongodb@marc21@67968877080005501"')

        records = [record for record in chunk.get_records() if record.is_deleted() is True]
        self.assertEqual(len(records), 1, 'Must be 1 deleted record')
        self.assertEqual(records[0].mms_id, '991000347589705501', 'MMS ID should be 991000347589705501')

    def test_bad_Chunk_5(self):
        oai_set = harvester.OaiSet(set_name='slsp_mongodb', base_url=base_url)
        chunk = harvester.Chunk(oai_set=oai_set,
                                file_path='harvested_data/test/bad_chunk_5.xml')
        token = chunk.get_resumption_token()
        self.assertIsNone(token, 'Token should be None')
        chunk.save()
        self.assertTrue('Unauthorized access to the OAI services – please contact the system administrator for assistance' in chunk.error_messages, 'Chunk should have error messages')

    def test_harvest_from(self):
        oai_set = harvester.OaiSet(set_name='slsp_mongodb', base_url=base_url)
        for chunk in oai_set.get_next_chunk(from_time='2024-06-15', to_time='2024-06-18'):
            break

        self.assertEqual(len(chunk.get_records()), 900, 'Chunk should have 900 records')


class TestRecords(unittest.TestCase):
    def test_good_record_1(self):
        with open('records/record_1.xml', 'r') as f:
            xml_data = f.read()
        record = harvester.XmlRecord(xml_data=xml_data)
        self.assertEqual(record.mms_id, '991171056281605501', 'MMS ID should be 991171056281605501')

        json_record = record.to_json()

        self.assertEqual(json_record.data['marc']['001'], '991171056281605501', 'MMS ID should be 991171056281605501')

    def test_good_record_2(self):
        with open('records/record_1.xml', 'rb') as f:
            xml_data = f.read()
        record = harvester.XmlRecord(xml_data=xml_data)
        self.assertEqual(record.mms_id, '991171056281605501', 'MMS ID should be 991171056281605501')

    def test_good_record_3(self):
        xml_data = etree.parse('records/record_1.xml').getroot()
        record = harvester.XmlRecord(xml_data=xml_data)
        self.assertEqual(record.mms_id, '991171056281605501', 'MMS ID should be 991171056281605501')

    def test_bad_record_1(self):
        with open('records/bad_record_1.xml', 'r') as f:
            xml_data = f.read()
        record = harvester.XmlRecord(xml_data=xml_data)
        self.assertTrue(record.error, 'Record should have an error')
        self.assertEqual(record.mms_id, '991171056281605501', 'MMS ID should be 991171056281605501')

    def test_bad_record_2(self):
        with open('records/bad_record_1.xml', 'rb') as f:
            xml_data = f.read()
        record = harvester.XmlRecord(xml_data=xml_data)
        self.assertTrue(record.error, 'Record should have an error')
        self.assertEqual(record.mms_id, '991171056281605501', 'MMS ID should be 991171056281605501')
        self.assertIsNone(record.data, 'Data should be None')

    def test_deleted_record_1(self):
        with open('records/deleted_record.xml', 'r') as f:
            xml_data = f.read()
        record = harvester.XmlRecord(xml_data=xml_data)
        self.assertTrue(record.is_deleted(), 'Record should be deleted')
        # self.assertEqual(record.mms_id, '991171056281605501', 'MMS ID should be 991171056281605501')
        # self.assertIsNone(record.data, 'Data should be None')
        # self.assertEqual(record.error_messages[0], 'Record is deleted', 'Error message should be "Record is deleted"')

    def test_deleted_record_2(self):
        with open('records/record_1.xml', 'r') as f:
            xml_data = f.read()
        record = harvester.XmlRecord(xml_data=xml_data)
        self.assertFalse(record.is_deleted(), 'Record should be deleted')

    def test_deleted_record_3(self):
        with open('records/deleted_record.xml', 'r') as f:
            xml_data = f.read()
        record = harvester.XmlRecord(xml_data=xml_data).to_json()
        self.assertTrue(hasattr(record, 'deleted'), 'Record should be deleted')

        with open('records/record_1.xml', 'r') as f:
            xml_data = f.read()
        record = harvester.XmlRecord(xml_data=xml_data).to_json()
        self.assertFalse(record.data.get('status') == 'deleted', 'Record should not be deleted')

    def test_eq_records(self):
        with open('records/record_1.xml', 'r') as f:
            xml_data = f.read()
        with open('records/record_2.xml', 'r') as f:
            xml_data2 = f.read()
        record1 = harvester.XmlRecord(xml_data=xml_data).to_json()
        record2 = harvester.XmlRecord(xml_data=xml_data).to_json()
        record3 = harvester.XmlRecord(xml_data=xml_data2).to_json()
        self.assertTrue(record1 == record2, 'Records should be equal')
        self.assertFalse(record1 == record3, 'Records should not be equal')

    def test_to_json_1(self):
        with open('records/record_1.xml', 'r') as f:
            xml_data = f.read()
        record = harvester.XmlRecord(xml_data=xml_data).to_json()
        self.assertEqual(record.data['marc']['001'],
                         '991171056281605501',
                         'MMS ID should be 991171056281605501')
        self.assertEqual(record.data['marc']['245'][0]['sub'][1]['c'],
                         'edited by Aline Godfroid and Holger Hopp',
                         'Title doesnt match')

    def test_to_json_2(self):
        with open('records/record_3.xml', 'r') as f:
            xml_data = f.read()
        record = harvester.XmlRecord(xml_data=xml_data).to_json()
        self.assertIsNotNone(record.data['marc'].get('ERROR'), 'Error tag should be present')
        self.assertEqual(record.data['marc']['ERROR'][0]['sub'][0]['a'],
                         '370',
                         'Content of error tag should be 370')
        self.assertEqual(record.data['marc']['082'][1]['sub'][0]['ERROR'],
                         '420',
                         'Content of error subfield code ERROR should be 420')
        self.assertEqual(record.data['marc']['082'][2]['ind1'],
                         'ERROR',
                         'Content of indicator 2 should be ERROR')
        self.assertTrue(record.data_error, 'Data error should be True')
        self.assertGreater(len(record.data_error_messages), 0, 'Data error messages should be present')

    def test_to_archive(self):
        with open('records/record_1.xml', 'r') as f:
            xml_data = f.read()
        record = harvester.XmlRecord(xml_data=xml_data).to_json()
        archive_record = record.to_archive()
        self.assertEqual(archive_record.data['versions'][0]['marc']['001'],
                         '991171056281605501',
                         'MMS ID should be 991171056281605501')


class test_tools(unittest.TestCase):

    def test_check_free_space(self):
        self.assertFalse(tools.check_free_space('harvested_data', low_limit=10000, error_limit=10000), 'There should not be enough free space')
        self.assertTrue(tools.check_free_space('harvested_data', low_limit=10, error_limit=10),
                         'There should  be enough free space')

    def test_get_newest_chunks_list(self):
        oai_set = harvester.OaiSet(set_name='slsp_mongodb', base_url=base_url)

        i = 0
        for chunk in oai_set.get_next_chunk():
            chunk.save()
            i += 1
            if i >= 2:
                break

        chunks = tools.get_newest_chunks_list()
        self.assertGreater(len(chunks), 0, 'There should be chunks')
        self.assertTrue(all([os.path.exists(chunk) for chunk in chunks]), 'All chunks should exist')


class test_mongodb(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)
        mongo.db[mongo.active_col].drop()
        mongo.db[mongo.hist_col].drop()
        mongo.db[mongo.task_col].drop()
        mongo.db['test1'].drop()
        mongo.db['test2'].drop()
        mongo.db['test3'].drop()
        mongo.client.close()

    def test_mongo(self):
        mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)
        self.assertFalse(mongo.error, 'There should not be an error')

        mongo.test = mongo.db['test1']
        mongo.test.insert_one({'test': 'test'})
        self.assertEqual(mongo.test.find_one()['test'], 'test', 'Test should be "test"')

        mongo.test.delete_one({'test': 'test'})
        self.assertIsNone(mongo.test.find_one(), 'Test should be None')

        mongo.client.close()

    def test_mongo_get_record(self):
        mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)
        with open('records/record_1.xml', 'r') as f:
            xml_data = f.read()
        record = harvester.XmlRecord(xml_data=xml_data).to_json()
        mongo.db['test2'].insert_one(record.data)
        record = mongo.get_record('991171056281605501', 'test2')
        self.assertIsNotNone(record, 'Record should not be None')
        self.assertEqual(record.data['marc']['001'],
                         '991171056281605501',
                         'MMS ID should be 991171056281605501')
        mongo.client.close()

    def test_mongo_insert_record(self):
        oai_set = harvester.OaiSet(set_name='slsp_mongodb', base_url=base_url)
        chunk = iter(oai_set.get_next_chunk()).__next__()

        mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)

        json_record = None

        for xml_record in chunk.get_records():
            if xml_record.error:
                continue
            json_record = xml_record.to_json()
            if not json_record.error:
                mongo.insert_record(json_record, mongo.active_col)

        nb_records = mongo.db[mongo.active_col].count_documents({})
        self.assertGreaterEqual(nb_records, 900, 'Number of records should be 900 or more')
        mms_ids = mongo.get_all_mms_ids(mongo.active_col)
        self.assertGreaterEqual(len(mms_ids), 900, 'Number of records should be 900 or more')
        self.assertIn(json_record.mms_id, mms_ids, 'MMS ID should be in the list')

        mongo.client.close()

    def test_mongo_insert_many_records(self):
        oai_set = harvester.OaiSet(set_name='slsp_mongodb', base_url=base_url)
        chunk_iter = iter(oai_set.get_next_chunk())
        _ = chunk_iter.__next__()
        chunk = chunk_iter.__next__()

        mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)

        bulk_records = set()
        for xml_record in chunk.get_records():
            if xml_record.error:
                continue
            json_record = xml_record.to_json()
            if not json_record.error:
                bulk_records.add(json_record)

        mongo.insert_many_records(bulk_records, 'test3')

        nb_records = mongo.db['test3'].count_documents({})
        self.assertGreaterEqual(nb_records, 900, 'Number of records should be 900 or more')
        mms_ids = mongo.get_all_mms_ids('test3')
        self.assertGreaterEqual(len(mms_ids), 900, 'Number of records should be 900 or more')
        self.assertIn(list(bulk_records)[-1].mms_id, mms_ids, 'MMS ID should be in the list')

        mongo.client.close()

    def test_mongo_archive_record_1(self):
        with open('records/record_4_v1.xml', 'r') as f:
            xml_data1 = f.read()
        record1 = harvester.XmlRecord(xml_data=xml_data1).to_json()

        with open('records/record_4_v2.xml', 'r') as f:
            xml_data2 = f.read()
        record2 = harvester.XmlRecord(xml_data=xml_data2).to_json()

        mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)

        mongo.archive_record(record1)
        mongo.archive_record(record2)

        nb_versions = len(mongo.get_record('991171842027005501', mongo.hist_col).data['versions'])
        self.assertEqual(nb_versions, 2, 'Number of versions should be 2')
        mongo.archive_record(record1)
        nb_versions = len(mongo.get_record('991171842027005501', mongo.hist_col).data['versions'])
        self.assertEqual(nb_versions, 2, 'Number of versions should be 2')

        mongo.client.close()

    def test_mongo_archive_record_2(self):
        with open('records/record_5_v1.xml', 'r') as f:
            xml_data1 = f.read()
        record1 = harvester.XmlRecord(xml_data=xml_data1).to_json()

        with open('records/record_5_v2.xml', 'r') as f:
            xml_data2 = f.read()
        record2 = harvester.XmlRecord(xml_data=xml_data2).to_json()

        mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)

        mongo.insert_record(record1, mongo.active_col)
        if record1.p_date > record2.p_date:
            mongo.archive_record(record2)
        else:
            mongo.archive_record(record1)
            mongo.update_record(record2, mongo.active_col)

        with open('records/record_5_v3.xml', 'r') as f:
            xml_data3 = f.read()
        record3 = harvester.XmlRecord(xml_data=xml_data3)

        mongo.delete_record(record3.mms_id, mongo.active_col)

        hist_record = mongo.get_record(record3.mms_id, mongo.hist_col)
        self.assertIsNotNone(hist_record, 'Record should not be None')

        active_record = mongo.get_record(record3.mms_id, mongo.active_col)
        self.assertIsNone(active_record, 'Record should be None')
        mongo.client.close()

    def test_mongo_archive_record_3(self):
        with open('records/record_6_v1.xml', 'r') as f:
            xml_data1 = f.read()
        record1 = harvester.XmlRecord(xml_data=xml_data1).to_json()

        with open('records/record_6_v2.xml', 'r') as f:
            xml_data2 = f.read()
        record2 = harvester.XmlRecord(xml_data=xml_data2).to_json()

        mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)

        mongo.insert_record(record1, mongo.active_col)

        temp_rec = mongo.get_record(record1.mms_id, mongo.active_col)

        self.assertTrue(temp_rec.data_error, 'Data error should be True')
        self.assertEqual(len(temp_rec.data_error_messages), 3, 'Data error messages should be present')

        mongo.archive_record(record1)
        mongo.update_record(record2, mongo.active_col)

        # In active col a new record is available with only 1 error
        temp_rec = mongo.get_record(record1.mms_id, mongo.active_col)
        self.assertTrue(temp_rec.data_error, 'Data error should be True')
        self.assertEqual(len(temp_rec.data_error_messages), 1, 'Data error messages should be present')

        # In history col the first record is archived with 3 errors
        temp_rec = mongo.get_record(record1.mms_id, mongo.hist_col)
        self.assertTrue(temp_rec.data_error, 'Data error should be True')
        self.assertEqual(len(temp_rec.data_error_messages), 3, 'Data error messages should be present')

        with open('records/record_6_v3.xml', 'r') as f:
            xml_data3 = f.read()
        record3 = harvester.XmlRecord(xml_data=xml_data3).to_json()
        mongo.archive_record(record3)
        mongo.update_record(record3, mongo.active_col)

        # In active col a new record is available with only 1 error
        temp_rec = mongo.get_record(record1.mms_id, mongo.active_col)
        self.assertFalse(temp_rec.data_error, 'Data error should be False')
        self.assertEqual(len(temp_rec.data_error_messages), 0, 'Data error messages should be present')

        # In history col the first record is archived with 3 errors
        temp_rec = mongo.get_record(record1.mms_id, mongo.hist_col)
        self.assertFalse(temp_rec.data_error, 'Data error should be False')
        self.assertEqual(len(temp_rec.data_error_messages), 0, 'Data error messages should be present')

        # #
        # # mongo.delete_record(record3.mms_id, mongo.active_col)
        # #
        # # hist_record = mongo.get_record(record3.mms_id, mongo.hist_col)
        # # self.assertIsNotNone(hist_record, 'Record should not be None')
        # #
        # # active_record = mongo.get_record(record3.mms_id, mongo.active_col)
        # # self.assertIsNone(active_record, 'Record should be None')
        mongo.client.close()

    def test_update_workflow(self):
        chunk = harvester.Chunk(oai_set=harvester.OaiSet(set_name='slsp_mongodb', base_url=base_url),
                                file_path='harvested_data/test/chunk_slsp_mongodb_00002.xml')

        mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)

        for record in chunk.get_records():
            if record.error:
                continue
            json_record = record.to_json()
            if not json_record.error:
                mongo.update_workflow(json_record)

        record = mongo.get_record('991171133322905501', mongo.active_col)
        self.assertEqual(record.data['u_date'],
                         datetime(2024, 6, 18, 15, 7, 29),
                         'Date should be 2024-06-18 15:07:29')

        record = mongo.get_record('991171133322905501', mongo.hist_col)
        self.assertEqual(len(record.data['versions']), 2, 'Number of versions should be 2')

        record = mongo.get_record('991171847533305501', mongo.active_col)
        self.assertIsNone(record, 'Record should be None because deleted')

        record = mongo.get_record('991171847533305501', mongo.hist_col)
        self.assertIsNotNone(record, 'Record should not be None in history')

        self.assertTrue(record.deleted, 'Record should be deleted')

        mongo.client.close()

    def test_create_new_task_1(self):
        mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)
        mongo.db[mongo.task_col].drop()
        mongo.get_in_process_task(new_task=True)
        self.assertEqual(mongo.db[task_col].count_documents({}), 1, 'Number of documents should be 1')
        task = mongo.get_in_process_task()
        task.data['end_time'] = datetime(2050, 12, 31, 0, 0, 0)
        task.update()

        task = mongo.get_in_process_task()
        self.assertEqual(task.data['end_time'], datetime(2050, 12, 31, 0, 0, 0), 'End time should be 2050-12-31')

        task.data['in_process'] = False
        task.update()

        task = mongo.get_in_process_task()
        self.assertTrue(task.error, 'Task error should be True')

        mongo.get_in_process_task(new_task=True)
        self.assertEqual(mongo.db[task_col].count_documents({}), 2, 'Number of documents should be 2')

        # Create new stat before the previous one is finished => error
        mongo.get_in_process_task(new_task=True)
        self.assertEqual(mongo.db[task_col].count_documents({}), 2, 'Number of documents should be 2')
        self.assertEqual(mongo.db[task_col].count_documents({'error': True}), 0, 'Number of documents with error should be 0')

        mongo.client.close()

    def test_create_new_stat_2(self):
        mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)
        mongo.db[mongo.task_col].drop()
        mongo.get_in_process_task(new_task=True)
        task = mongo.get_in_process_task()
        task.data['end_time'] = task.data['start_time'] + timedelta(hours=1)
        task.data['in_process'] = False
        task.update()

        task = {'start_time': datetime.now() + timedelta(days=3),
                'end_time': datetime.now() + timedelta(days=4),
                'critical_error': False,
                'critical_error_messages': [],
                'data_error_messages': [],
                'in_process': False}

        mongo.db[mongo.task_col].insert_one(task)

        task = {'start_time': datetime.now() + timedelta(days=5),
                'end_time': datetime.now() + timedelta(days=6),
                'critical_error': True,
                'error_messages': [],
                'data_error_messages': []}

        mongo.db[mongo.task_col].insert_one(task)

        d = mongo.get_harvesting_from_time()
        self.assertEqual(d.day, (datetime.now() + timedelta(days=3)).day, 'Date should be 3 days from now')

        mongo.client.close()

    def test_close_task(self):
        mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)
        mongo.db[mongo.task_col].drop()
        task = mongo.get_in_process_task(new_task=True)
        time.sleep(3)
        self.assertIsNone(task.data['end_time'], 'End time should be None')
        task.close()

        self.assertIsNotNone(task.data['end_time'], 'End time should not be None')
        t = mongo.get_in_process_task()
        self.assertTrue(t.error, 'Task should have an error')
        self.assertEqual(t.data, dict(), 'Data should be empty')

    def test_format_and_access(self):
        with open('records/record_1.json', 'rb') as f:
            data = json.load(f)
        rec = JsonRecord(data)
        self.assertEqual(rec.format, 'Image')

        data['marc']['leader'] = data['marc']['leader'][:6] + 'am' + data['marc']['leader'][8:]
        rec = JsonRecord(data)
        self.assertEqual(rec.format, 'Book')
        self.assertEqual(rec.access, 'P')

        data['marc']['008'] = data['marc']['008'][:23] + 'o' + data['marc']['008'][:24]
        rec = JsonRecord(data)
        self.assertEqual(rec.format, 'Book')
        self.assertEqual(rec.access, 'O')

    def test_filter_versions(self):
        with open('records/record_2.json', 'rb') as f:
            data = json.load(f)

        for v in data['versions']:
            v['p_date'] = datetime.fromisoformat(v['p_date']['$date'].replace("Z", "+00:00"))

        rec = ArchiveJsonRecord(data)
        rec.filter_versions()
        self.assertEqual(len(rec.data['versions']), 4)

    @classmethod
    def tearDownClass(cls):
        mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)
        mongo.db[mongo.active_col].drop()
        mongo.db[mongo.hist_col].drop()
        mongo.db[mongo.task_col].drop()
        mongo.db['test1'].drop()
        mongo.db['test2'].drop()
        mongo.db['test3'].drop()
        mongo.client.close()


if __name__ == '__main__':
    unittest.main()
