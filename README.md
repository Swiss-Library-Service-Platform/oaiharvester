# OAI harvester
* Author: Raphaël Rey (raphael.rey@slsp.ch)
* Date: 2024-07-01
* Version: 1.0.1

## Description
This application is used to harvest Alma
and to create a copy of the data in MongoDB
database. It uses OAI pmh to harvest the
data and ensures versioning.

## Usage
The system is supposed to be used with
a periodic scheduling, for example once in
a week. A cron table can start the harvesting
process with the following command:

```bash
python3 workflow.py
```

It will harvest the data from Alma and store
it into the Mongo database.

It is also possible to skip the harvesting
and to load already harvested data:

```bash
python3 workflow.py --directory harvested_data/OaiSet_slsp_mongodb_20240701
```

The harvested data should be in normal Marc21 format. If
there are some errors like missing indicators or bad subfields
codes, the system will replace bad chars with `ERROR` string
and add a line in the `data_error_messages` of the task.

It simplifies the correction.

## Installation
An environment variable is required to store
the `MONGO_URI` for the MongoDB connection. It
is possible to use a `.env` file.

Required libraries are in `requirements.txt`.
```bash
pip install -r requirements.txt
```

## Configuration
The configuration is to be defined in
the `config.cfg` file for production
and in `config_test.cfg` for
the unittest.

* BASE_URL: OAI pmh endpoint to use
* SET_NAME: set name of the set to harvest
* DB_NAME: name of the MongoDB to use
* ACTIVE_COL: main collection to store active data
* HIST_COL: collection to store old versions of the records
* TASK_COL: collection of the tasks to monitor the update odf the data

## Data structure

### List of collections
| Collections     | Description                                                                                             |
|:----------------|:--------------------------------------------------------------------------------------------------------|
| nz_records      | List of bib records of network zone                                                                     |
| nz_records_hist | List of record with old versions and deleted records                                                    |
| nz_tasks        | List of tasks, contains statistics and state of the task, useful to monitor the updates of the database |

### Collection ACTIVE_COL
| Fields                            | Description                                                                                                                                                                                              |
|:----------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| p_date                            | Publication date, datestamp of the oai export file                                                                                                                                                       |
| c_date                            | Creation date of the record                                                                                                                                                                              |
| u_date                            | Update date of the record, it's the time when the record is updated in Alma, it's not the same as the publication date. A record can be updated several times in a week, but can be published only once. |
| sup                               | Boolean indicating if it’s a suppressed record, suppressed record means suppressed from discovery. Deleted records are only found in the nz_records_hist collection.                                     |
| data_error                        | Boolean, only available when error is in the data.                                                                                                                                                       |
| data_error_messages               | List of strings with description of the errors, only available in case of errors related to the data.                                                                                                    |
| mms_id                            | Network zon MMS ID, same value as marc.001.                                                                                                                                                              |
| marc.leader                       | Leader field, content is a simple string.                                                                                                                                                                |
| marc.00X                          | Controlfields, the content of the field is displayed directly in a string. Tag is the key of the marc dictionnary.                                                                                       |
| marc.&lt;tag&gt;                  | Datafields, the content is in a array. A same tag can contain more than one field. All datafield with same tag are in a list.                                                                            |
| marc.&lt;tag&gt;.ind1             | Indicator 1 of a datafield. If empty value will be “ “.                                                                                                                                                  |
| marc.&lt;tag&gt;.ind2             | Indicator 2 of a datafield. If empty value will be “ “.                                                                                                                                                  |
| marc.&lt;tag&gt;.sub.&lt;code&gt; | Subfields, each datafield can have one or mor subfields. Subfields are stored in lists. Content of the subfield is direct value of the code.                                                             |

### Collection HIST_COL
Most of the fields are the sames as in ACTIVE_COL collection. The only difference
is the addition of the `deleted` field. This field is a boolean indicating if the
record has been deleted. It's useful to know if the record has been deleted or not.

| Fields                 | Description                                               |
|:-----------------------|:----------------------------------------------------------|
| deleted                | Boolean indicating if the records has been deleted.       |
| versions               | List of all versions, newest is in the bottom of the list |

### Collection TASK_COL
| Fields                   | Description                                                                                        |
|--------------------------|----------------------------------------------------------------------------------------------------|
| start_time               | Time when task started                                                                             |
| end_time                 | Time when task ended                                                                               |
| chunk_directory          | Path to the chunk directory                                                                        |
| critical_error           | Boolean, true in case of a blocking error                                                          |
| critical_error_messages  | List of string explaining type of error                                                            |
| data_error_messages      | List or error messages related to the data. They ar useful to clean the data but are not blocking. |
| nb_chunks                | Number of chunks                                                                                   |
| duration                 | Duration of the update task in minutes                                                             |
| nb_records_at_start_time | Number of records in nz_records collection when task started.                                      |
| nb_records_at_end_time   | Number of records in nz_records collection when task ended.                                        |

## Testing
The unittests are in the `test` directory. They
can be run with the following command:

```bash
python3 -m unittest 
``` 

Test data are available in the `test/harvested_data/test` folder.

Single records are in the `test/records` folder.

## License
GNU General Public License v3.0