# Bank Data Pipeline Project

After the repo has been cloned, to run the pipeline, navigate to the ```src``` directory. Once in the directory it is recommended to create a virtual environment.

To create a virtual environment run the following commands:

Create a Virtual env:

```$ python3.10 -m venv .venv```

Activate venv:

```source .venv/bin/activate```

Install libraries:

```$ pip install -r requirements.txt```

Once the .venv is active and you are in the working directory, the job can be run by executing the main.py file which will execute all steps of the pipeline

```python main.py```


The job will generate a new directory named `outputs` inside the src folder that will contain subdirectories with the outputs from each step of the pipeline, raw, bronze, silver, gold, and samples.

## Project Notes

The source data is included in the git repository (the large transaction file has been compressed) inside the ```ext_data``` directory. 
This is to mimic the ingest step in a production pipeline that will be fetching data from an external source and moving it to the ```raw``` environment. The job uses a ```config``` file to identify which files to fetch from the external data source and then uses the information in this file, specific to each ingested file, to perform the second step of the pipeline, the cleaning. 

### Nulls
During the cleaning step, nulls are handled by setting:

Datetime fields to ```1900-01-01``` 

String/Object fields to ```none```

Numeric nulls are left intentionally blank at this step so as to not interrupt any mathematical functions with zeroes at this step in the data flow. 

After aggregations are completed in the gold data level, zeroes are introduced to allow proper calculations.

### Future files
The ingestion portion of this pipeline was set up so that if any future files are available, the ingestion job will concatenate matching file types to the existing dataframes, then remove duplicates. This allows for any number of new files to be received. The `ingest.py` file has a global variable `FILE_VERSION` that can be altered to test the arrival of new files by appending the file version to the file name, mimicking an updated version of a file. 

Updated files can contain additional fields without breaking any existing data flows. In anticipation of the new column, `avg_salary_98` in the `DISTRICT` table, a lookup has already been added to the column table in the `config.py` file. All configurations are centrally located in the `config.py` file to make them easy to update. New file types can also be added by adding an entry to the `FILE_LIST` entry. 

### Additional Notes
Pandas was chosen for the data manipulation tool to keep things simple (fewer library imports) as well as out of familiarity. 
If given more time, I would have added testing, dependency logic (a few of the models are dependent on other models, and as of now, there is nothing other than order of function calls to ensure upstream tables exist at the time of downstream table creation), error handling, and logging. The ingestion portion of the pipe was designed so that files are not ingested if they already exist in the raw directory (unless the `config.py` variable: `FORCE_REFRESH` is marked as `True`) due to the fact that the ingestion step can be the most expensive with regard to time and bandwidth when dealing with large files. 