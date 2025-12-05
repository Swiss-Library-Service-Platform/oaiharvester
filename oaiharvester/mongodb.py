import logging
from pymongo import MongoClient
import os
from typing import Callable, Any, Optional, Set
from .records import JsonRecord, ArchiveJsonRecord
from datetime import datetime, date, timedelta


def check_error(func: Callable) -> Callable:
    """
    Check if there is an error with the MongoDB client before executing the function

    Parameters
    ----------
    func : Callable
        Function to execute

    Returns
    -------
    Callable
    """

    def wrapper(*args, **kwargs) -> Any:
        """
        Wrapper function to check if there is an error with the MongoDB client before executing the function
        """
        if args[0].error is True:
            logging.error(f'Error to the Client MongoDB, "{args[0].__class__.__name__}" skipped')
            return
        return func(*args, **kwargs)

    return wrapper


class Mongo:
    """
    MongoDB client

    Attributes:
    ----------
    active_col : str
        Collection name
    hist_col : str
        Collection for the previous records versions
    error : bool
        Error flag
    error_messages : List[str]
    client : MongoClient
        MongoDB client
    db_name : str
        Name of the database
    db : Database
        MongoDB database


    """

    def __init__(self, db_name: str, active_col: str, hist_col: str, task_col: str):
        """
        Initialize the MongoDB client

        Parameters:
        ----------
        active_col : str
            Collection name
        hist_col : str
            Collection name for the previous records versions
        task_col : str
            Collection name for the statistics related to the tasks
        """
        self.error = False
        self.error_messages = []
        self.db_name = db_name

        if not os.getenv('MONGO_URI'):
            logging.critical('MONGO_URI environment variable is not set')
            self.error_messages.append('MONGO_URI environment variable is not set')
            self.error = True
            return
        try:
            self.client = MongoClient(os.getenv('MONGO_URI'))
        except Exception as e:
            logging.critical(f'Error connecting to MongoDB: {e}')
            self.error_messages.append(f'Error connecting to MongoDB: {e}')
            self.error = True
            return
        self.db = self.client[db_name]
        self.active_col = active_col
        self.hist_col = hist_col
        self.task_col = task_col

    @check_error
    def get_record(self, mms_id: str, col: str) -> Optional[JsonRecord]:
        """
        Get a record from the MongoDB database

        Parameters
        ----------
        mms_id : str
            MMS ID of the record
        col : str
            Collection name

        Returns
        -------
        JsonRecord
        """
        try:
            rec = self.db[col].find_one({'mms_id': mms_id})
        except Exception as e:
            logging.critical(f'Error getting record {mms_id}: {e}')
            self.error_messages.append(f'Error getting record {mms_id}: {e}')
            self.error = True
            return None

        if rec is None:
            logging.warning(f'Record {mms_id} not found in the {col} collection')
            return None

        if col == self.hist_col:
            return ArchiveJsonRecord(dict(rec))

        else:
            return JsonRecord(dict(rec))

    @check_error
    def insert_record(self, record: JsonRecord, col: str) -> None:
        """
        Insert a record into the MongoDB database

        Parameters
        ----------
        record : JsonRecord
            Record to insert
        col : str
            Collection name
        """
        try:
            self.db[col].insert_one(record.data)
            return
        except Exception as e:
            logging.error(f'Error inserting record {record.data["mms_id"]}: {e}')

        try:
            if self.get_record(record.mms_id, col) is not None:
                logging.warning(f'Record {record.mms_id} already exists in the {col} collection => cannot be inserted')
                return
            else:
                logging.error(f'Unknown error with {record.mms_id}')
        except Exception as e:
            logging.critical(f'{record.data["mms_id"]}: {e}')
            self.error = True
            self.error_messages.append(f'{record.data["mms_id"]}: {e}')

    @check_error
    def insert_many_records(self, records: Set[JsonRecord], col: str) -> None:
        """
        Insert a record into the MongoDB database

        Parameters
        ----------
        records : set of JsonRecord
            Set of records to insert
        col : str
            Collection name
        """
        json_data = [record.data for record in records]

        try:
            self.db[col].insert_many(json_data)
            logging.info(f'{len(json_data)} records inserted in {col} collection')
            return
        except Exception as e:
            logging.error(f'Error inserting {len(json_data)} records: {e}')

        # Try ton insert single records
        for record in records:
            self.insert_record(record, col)

    @check_error
    def update_record(self, record: JsonRecord, col: str) -> None:
        """
        Update a record in the MongoDB database

        Parameters
        ----------
        record : JsonRecord
            Record to update
        col : str
            Collection name
        """
        try:
            self.db[col].replace_one({'mms_id': record.data['mms_id']}, record.data)
        except Exception as e:
            logging.critical(f'Error updating record {record.data["mms_id"]}: {e}')
            self.error_messages.append(f'Error updating record {record.data["mms_id"]}: {e}')
            self.error = True

    @check_error
    def archive_record(self, record: JsonRecord) -> None:
        """
        Archive a record in the MongoDB database

        Parameters
        ----------
        record : JsonRecord
            Record to archive
        """
        history_record = self.get_record(record.mms_id, self.hist_col)

        if history_record is not None:
            if hasattr(record, 'deleted') and record.deleted is True:
                history_record.deleted = True
            else:
                history_record.deleted = False

            logging.info(f'{repr(record)}:Record {record.mms_id} already exists in the {self.hist_col} collection')
            history_record.add_record_to_archive(record)
            try:
                self.update_record(history_record, self.hist_col)
                logging.info(f'{repr(record)}: updated record in {self.hist_col} collection')
            except Exception as e:
                logging.critical(f'Error archiving record {record.data["mms_id"]}: {e}')
                self.error_messages.append(f'Error archiving record {record.data["mms_id"]}: {e}')
                self.error = True

        else:
            logging.warning(f'{repr(record)}: does not exist in the {self.hist_col} collection')
            try:
                self.insert_record(record.to_archive(), self.hist_col)
                logging.info(f'{repr(record)}: added record to {self.hist_col} collection')
            except Exception as e:
                logging.critical(f'Error archiving record {record.data["mms_id"]}: {e}')
                self.error_messages.append(f'Error archiving record {record.data["mms_id"]}: {e}')
                self.error = True

    @check_error
    def delete_record(self, mms_id: str, col: str) -> None:
        """
        Delete a record from the MongoDB database

        Parameters
        ----------
        mms_id : str
            MMS ID of the record
        col : str
            Collection name
        """
        try:
            self.db[col].delete_one({'mms_id': mms_id})
            logging.info(f'Record {mms_id} deleted from {col} collection')
        except Exception as e:
            logging.critical(f'Error deleting record {mms_id}: {e}')
            self.error_messages.append(f'Error deleting record {mms_id}: {e}')
            self.error = True

    @check_error
    def delete_workflow(self, record: JsonRecord) -> None:
        """
        Delete a record from the MongoDB database

        Parameters
        ----------
        record : JsonRecord
            Record to delete
        """
        existing_record = self.get_record(record.mms_id, self.active_col)

        if existing_record is None:
            logging.info(f'{repr(record)} does not exist in the {self.active_col} collection')
            return None

        logging.info(f'{repr(record)}: deleted record in the {self.active_col} collection')

        if record.p_date < existing_record.p_date:
            # New oai record is older than the existing record
            logging.warning(f'{repr(record)}: a newer record exists in the {self.active_col} '
                            f'collection => impossible to delete it')
            return None
        existing_record.deleted = True
        self.archive_record(existing_record)
        self.delete_record(record.mms_id, self.active_col)

    @check_error
    def update_workflow(self, record: JsonRecord, no_insert: Optional[bool] = False) -> None:
        """
        Insert a record into the MongoDB database

        Parameters
        ----------
        record : JsonRecord
            Record to insert
        no_insert : bool, optional
            Do not insert the record if it does not exist in the active collection, default is False
        """

        # Get existing active record
        existing_record = self.get_record(record.data['mms_id'], self.active_col)

        # Check if record is deleted
        if hasattr(record, 'deleted') and record.deleted is True:
            self.delete_workflow(record)
            return

        # Record already exists in active collection, same datestamp, same mms_id => do nothing
        if existing_record is not None and existing_record == record:
            logging.info(f'Record {record.mms_id} already exists in the database with the same '
                         f'datestamp {record.data["p_date"]}')
            return None

        # No existing record in active collection with the MMS_ID => must be inserted
        if existing_record is None and no_insert is False:
            self.insert_record(record, self.active_col)
            logging.info(f'{repr(record)}: added record to {self.active_col} collection')
            return None

        # When xml record is newer than the existing one => archive current + update with the new
        if record.p_date > existing_record.p_date:
            # New oai record is newer than the existing record
            self.archive_record(existing_record)
            self.update_record(record, self.active_col)
            logging.info(f'{repr(record)}: updated record in {self.active_col} collection, previous version archived')
            return None

        # Not common: xml record is older than the existing one => archive the xml one
        elif record.p_date < existing_record.p_date:
            # New oai record is older than the existing record
            self.archive_record(record)
            logging.info(f'{repr(record)}: archived record in {self.hist_col} collection,'
                         f'newer record is available in {self.active_col} collection')
            return None

    @check_error
    def get_all_mms_ids(self, col: str) -> set:
        """
        Get all MMS IDs from the MongoDB database

        Parameters
        ----------
        col : str
            Collection name

        Returns
        -------
        list
        """
        try:
            return set(doc['mms_id'] for doc in self.db[col].find({}, {'mms_id': 1}))

        except Exception as e:
            logging.critical(f'Error getting all MMS IDs: {e}')
            self.error_messages.append(f'Error getting all MMS IDs: {e}')
            self.error = True
            return set()

    def get_in_process_task(self, new_task: bool = False) -> Optional['Task']:
        """
        Get the current task

        Returns
        -------
        Task
        """
        return Task(self, new_task)

    @check_error
    def get_harvesting_from_time(self) -> Optional[date]:
        """
        Get the last harvesting date

        Returns
        -------
        date
        """
        try:
            tasks = self.db[self.task_col].find({'$and': [{'critical_error': False},
                                                          {'end_time': {'$ne': None}},
                                                          {'in_process': False}]},
                                                sort=[('start_time', -1)])
            task = next(tasks, None)
            if task is None or task['end_time'] is None:
                return date.today()
            return task['start_time'].date()
        except Exception as e:
            logging.critical(f'Error getting last harvesting date: {e}')
            self.error_messages.append(f'Error getting last harvesting date: {e}')
            self.error = True
            return None


class Task:
    """
    Class representing a task

    Properties:
    ----------
    error : bool
        Error flag (based on the MongoDB client error flag)

    Attributes:
    -----------
    mongodb : Database
        MongoDB database
    data : dict
        Task data
    """

    def __init__(self, mongo: Mongo, new_task: bool = False) -> None:
        """
        Initialize task

        Arguments:
        ----------
        mongo: Mongo
            Mongo object
        new_task: bool
            Create a new task
        """
        self.mongo = mongo
        self.error = False

        if new_task is True:
            self._create()

        # Useful to fetch it even after the task has been created to fetch the _id field
        self._data = self._get_in_process()

        if self._data is None or self.mongo.error is True:
            self.error = True

    def _create(self) -> None:
        """
        Create a new task
        """
        if self._get_in_process() is not None:
            logging.critical('Task already in process')
            self.mongo.error_messages.append(f'Task already in process')
            self.mongo.error = True
            return None

        task_data = {'start_time': datetime.now(),
                     'end_time': None,
                     'duration': None,
                     'nb_records_at_start_time': None,
                     'nb_records_at_end_time': None,
                     'in_process': True,
                     'chunk_directory': None,
                     'critical_error': False,
                     'critical_error_messages': [],
                     'data_error_messages': []}
        try:
            self.mongo.db[self.mongo.task_col].insert_one(task_data)
            return None

        except Exception as e:
            logging.critical(f'Error inserting statistics record: {e}')
            self.mongo.error_messages.append(f'Error inserting task record: {e}')
            self.mongo.error = True
            return None

    def _get_in_process(self) -> Optional[dict]:
        """
        Get the last task in process

        Returns
        -------
        dict
        """
        try:
            return self.mongo.db[self.mongo.task_col].find_one({'in_process': True})

        except Exception as e:
            logging.critical(f'Error getting last task in process: {e}')
            self.mongo.error_messages.append(f'Error getting last task in process: {e}')
            self.mongo.error = True
            return None

    @property
    def data(self) -> dict:
        """
        Get the task data

        This is used to avoid exceptions when task data are not found.

        Returns
        -------
        dict
        """
        if self._data is None or self.error is True:
            return {}
        else:
            return self._data

    @data.setter
    def data(self, value: dict) -> None:
        """
        Set the task data

        Parameters
        ----------
        value : dict
            Task data
        """
        self._data = value

    def update(self) -> Optional['Task']:
        """
        Update the current task

        Returns
        -------
        self

        """
        try:
            self.mongo.db[self.mongo.task_col].update_one({'_id': self.data['_id']}, {'$set': self.data})
            logging.info(f'Task record updated in {self.mongo.task_col} collection')
            return self
        except Exception as e:
            logging.critical(f'Error updating task record: {e}')
            self.mongo.error_messages.append(f'Error updating task record: {e}')
            self.mongo.error = True
            return None

    @check_error
    def add_data_error_message(self, message: str) -> None:
        """
        Add an error message to the MongoDB client

        Parameters
        ----------
        message : str
            Error message
        """
        try:
            self.mongo.db[self.mongo.task_col].update_one({'_id': self.data['_id']},
                                                          {'$push': {'data_error_messages': message}})
        except Exception as e:
            logging.critical(f'Error adding data error message: {e}')
            self.mongo.error = True

    @check_error
    def close(self):
        """
        Close the current task
        """
        logging.info(f'Closing task and close task in report collection')
        # Update stats
        active_mms_ids = self.mongo.get_all_mms_ids(self.mongo.active_col)

        # refresh data
        self._data = self._get_in_process()

        self.data['nb_records_at_end_time'] = len(active_mms_ids)
        self.data['end_time'] = datetime.now()
        self.data['duration'] = (self.data['end_time'] - self.data['start_time']) // timedelta(minutes=1)
        self.data['nb_records_at_end_time'] = len(active_mms_ids)
        self.data['in_process'] = False
        self.update()
