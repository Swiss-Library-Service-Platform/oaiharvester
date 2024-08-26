import os
from oaiharvester.harvester import OaiSet, Chunk
from oaiharvester.mongodb import Mongo
import oaiharvester.tools as tools
import configparser
import logging
from datetime import date

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)


###############
# Read config #
###############
config = configparser.ConfigParser()
config.read('config.cfg')
base_url = config['OAI_harvesting']['BASE_URL']
set_name = config['OAI_harvesting']['SET_NAME']
db_name = config['MongoDB']['DB_NAME']
active_col = config['MongoDB']['ACTIVE_COL']
hist_col = config['MongoDB']['HIST_COL']
task_col = config['MongoDB']['TASK_COL']

##################
# Define objects #
##################

oai_set = OaiSet(set_name=set_name, base_url=base_url)

mongo = Mongo(db_name=db_name, active_col=active_col, hist_col=hist_col, task_col=task_col)


#########################
# Harvest OAI Alma data #
########################
def harvest_data_from_alma(from_time: str, to_time: str) -> None:
    """
    Harvest data from Alma OAI-PMH endpoint

    Parameters
    ----------
    from_time : str
        Start date for harvesting
    to_time : str
        End date for harvesting
    """
    # No processing in case of error
    if oai_set.error is True or mongo.error is True:
        return

    # Start processing
    tools.configure_logger(job_name='harvest_alma', set_name=set_name)
    logging.info(f'Start harvesting set {set_name} from Alma OAI-PMH endpoint {base_url}')

    for i, chunk in enumerate(oai_set.get_next_chunk(from_time=from_time, to_time=to_time)):
        if oai_set.error is True:
            logging.critical('OAI-PMH error => exiting...')
            task = mongo.get_in_process_task()
            if task is None:
                break
            task.data['critical_error'] = True
            task.data['critical_error_messages'] += oai_set.error_messages
            task.update()
            task.close()
            break

        chunk.save()


##################
# Update MongoDB #
##################
def update_db() -> None:
    """
    Update MongoDB with harvested data
    """
    # No processing in case of error
    if oai_set.error is True or mongo.error is True:
        return None

    # Start processing
    tools.configure_logger(job_name='update_db', set_name=set_name)
    logging.info(f'Start inserting data from {set_name} to MongoDB')

    # Get all chunk names
    if tools.get_directory_param() is None:
        chunk_paths = list(tools.get_newest_chunks_list())
    else:
        chunk_paths = list(tools.get_chunk_list_from_directory(tools.get_directory_param()))

    # Update chunk_directory field
    if len(chunk_paths) > 0:
        task = mongo.get_in_process_task()
        task.data['chunk_directory'] = os.path.dirname(chunk_paths[0])
        task.update()

    logging.info(f'{len(list(chunk_paths))} chunks found')

    # Update stats
    task = mongo.get_in_process_task()
    task.data['nb_chunks'] = len(list(chunk_paths))
    task.update()

    # Get all mms_ids from active collection
    logging.info(f'Getting all mms_ids from {active_col} collection...')
    active_mms_ids = mongo.get_all_mms_ids(active_col)

    logging.info(f'{len(active_mms_ids)} mms_ids found in {active_col} collection')
    logging.info(f'Inserting data from {set_name} to MongoDB')

    # Update stats
    task = mongo.get_in_process_task()
    task.data['nb_records_at_start_time'] = len(active_mms_ids)
    task.update()

    # Used to collect new records to insert in bulk
    bulk_new_records = []

    # Process each chunk
    for i, chunk_path in enumerate(chunk_paths):

        # No processing in case of error
        if mongo.error is True:
            logging.critical('MongoDB error => exiting...')
            task = mongo.get_in_process_task()
            task.data['critical_error'] = True
            task.data['critical_error_messages'] += mongo.error_messages
            task.update()
            break

        logging.info(f'{i}/{len(list(chunk_paths))} - {chunk_path} processing')

        # Create chunk object, file_path is the path to the xml file
        chunk = Chunk(oai_set=oai_set,
                      file_path=chunk_path)

        # Iterate over records in the chunk
        for record in chunk.get_records():

            # Check data error of each xml record
            if record.error is True:
                for error_message in record.error_messages:
                    mongo.get_in_process_task().add_data_error_message(error_message)
                continue

            # Transform record to json to insert it in MongoDB
            json_record = record.to_json()

            # Check data error of each json record, json record with error are ignored => blocking error
            if json_record.error is not True:

                # Management of data error => not blocking error
                if json_record.data_error is True:
                    for error_message in json_record.data_error_messages:
                        mongo.get_in_process_task().add_data_error_message(error_message)

                # Check if the record is already in the active collection
                if hasattr(record, 'deleted') is False and record.mms_id not in active_mms_ids:
                    bulk_new_records.append(json_record)
                    active_mms_ids.add(record.mms_id)
                    continue

                # Update record in active collection, when deleted or updated
                mongo.update_workflow(json_record, no_insert=True)

        # Insert new records in bulk
        if len(bulk_new_records) >= 5000:
            mongo.insert_many_records(bulk_new_records, active_col)
            bulk_new_records = []

    # After processing all chunks, insert remaining new records in bulk
    if len(bulk_new_records) > 0:
        mongo.insert_many_records(bulk_new_records, active_col)

    logging.info(f'Inserting data from {set_name} to MongoDB finished')


#######################
# Update MongoDB data #
#######################
def main():
    """
    Main function to harvest data from Alma and update MongoDB
    """
    tools.configure_logger(job_name='task_workflow', set_name=set_name)
    logging.info(f'Starting workflow: harvesting data from {set_name} and updating MongoDB')
    # Create new task, will generate error if task already exists with status 'in_process'
    task = mongo.get_in_process_task(new_task=True)

    # No harvesting if directory is provided
    if tools.get_directory_param() is None and task.error is False and mongo.error is False:
        harvest_data_from_alma(from_time=mongo.get_harvesting_from_time().isoformat(),
                               to_time=date.today().isoformat())

    # Update MongoDB
    if task.error is False:
        update_db()

    tools.configure_logger(job_name='task_workflow', set_name=set_name)
    task.close()
    logging.info(f'Ended workflow: harvesting data from {set_name} and updating MongoDB')


if __name__ == '__main__':
    main()
