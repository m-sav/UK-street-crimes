# UK-street-crimes

This project processes UK street crime historical data and constructs a final structured dataset which is further analyzed and provides insights into the data. The dataset is saved as a csv file and also stored in a Postgres database.

## Requirements
- pandas
- numpy
- matplotlib.pyplot
- argparse
- os
- psycopg2

## Instructions
- In terminal, inside the repository path, run
```
docker-compose up --build db
```
  to run the database
  
- Connect to the Postgres database using the credentials:

```
USER = "postgres"
PASSWORD = "crimes"
host = "localhost"
port = "5430"
database = "ukcrimes"
```
  
- In the repository path, run the **processing.py** script with the following options:

 1) Extract the final structured dataset from scratch. Must provide a data source path,_e.g. 'data/'_ . This process may take a while to load.

```
python processing.py -mode extract -source data/
```

  The final structured dataset will be constructed and saved as a csv in the script's filepath and will also be stored in Postgres, in table 'crimes' in     public schema. In this table a new column with the geo location of each crime will also be generated.

  The final structured dataset will then be analyzed. The script will provide two plots and save them as png images along with several prints. By           uncommenting the plt.show() lines you can display the plots too.

  View the crimes table in Postgres database:
```
select * from crimes;
```

  2) Provide a structured dataset (_e.g. final_structured_dataset.csv_) and proceed directly to the analysis as described above.
```
python processing.py -mode from_csv -csv_file final_structured_data.csv
```
## Note

In file results.zip there is a brief report of the insights obtained by the final structured dataset along with some data visualizations.

