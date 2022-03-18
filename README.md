# Udacity Capstone Project

**Overview**: An ETL pipeline which gathers and transforms data on Yelp businesses, reviews and users, demographic data about cities in the United States, and the tax climate of each state.

**Table of Contents**

1. [Scope the Project and Gather Data](#scope-and-gathering-data)
2. [Data Assessment](#data-assessment)
    1. [Yelp Dataset](#yelp-dataset)
    2. [Demographics of U.S. Cities](#demographics-of-us-cities)
3. [Data Model Definition](#data-model-definition)
4. [ETL Process](#etl-process)
5. [How to use this project](#how-to-use-this-project)
6. [Concluding thoughts](#concluding-thoughts)

## Scope and Gather Data

### Scope

The idea of this project is to gather data from a couple of sources, which can be used together for analysis. In this project I plan to use data from Yelp, the Census Bureau, and the Tax Foundation. The end solution will provide data analysts and data scientists a structured data set and give them the ability to query and/or create more sophisticated models to answer interesting questions about businesses, reviewers or businesses, and control for things like the tax climate and demogrpahics. To do this, I will use the following tools: Python, Apache Airflow, PySpark

Once this data is compiled and transformed for use, the end user will be able to perform queries and create statistical models to answer several types of questions about businesses in the United States.

Example analyses/questions to answer with this dataset:
- What do popular businesses have in common when you control for demographics?
- Sentiment analysis of user reviews.
- What businesses suceed in the worst tax climate? Best tax climate?

### Gathering Data

There are three sources of data for this project:

1. [Yelp](https://www.yelp.com/dataset) provides a large dataset of JSON files for download.
2. [Open Data Soft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/), using data from the US Census Bureau, provides a JSON file of demographic information on US cities for download. 
3. [The Tax Foundation]('https://taxfoundation.org/2021-state-business-tax-climate-index/') created an index of the tax climate for each state. I gathered the data with a very simple Python script (`get_business_tax_climate_data.py`) and put the data in CSV format.

I've made the raw data available in this [Google Drive folder](https://drive.google.com/drive/folders/1E6D0AO8mE7OUWUtrbAPCjer8wKbQWzJu?usp=sharing). 

## Data Assessment

The following datasets -- the Yelp dataset and the demographic dataset on U.S. cities -- are for academic purposes. That is, they are for the purpose of this Udacity capstone project. More on that in each section of the data. To view some of the exploration of each dataset, please view the Jupyter Notebook in this repository (`data_exploration.ipynb`).

### Yelp Dataset

I obtained the following data from [Yelp](https://www.yelp.com/dataset): businesses, reviews, and users. Yelp provides a subset of their "business, reviews, and user data for use in personal, educational, and academic purposes. Available as JSON files, use it to teach students about databases, to learn NLP, or for sample production data while you learn how to make mobile apps."

For this project, we are using this data to create a data pipeline which cleans up the raw data, transforms them into SQL tables, and combines the data with the demographic data set discussed below.

Here's a sample of each Yelp data file used in this project.

#### yelp_academic_dataset_businesses.json

Contains business data including location data, attributes, and categories.

```json
{
    "business_id": "tnhfDv5Il8EaGSXZGiuQGg",
    "name": "Garaje",
    "address": "475 3rd St",
    "city": "San Francisco",
    "state": "CA",
    "postal_code": "94107",
    "latitude": 37.7817529521,
    "longitude": -122.39612197,
    "stars": 4.5,
    "review_count": 1198,
    "is_open": 1,
    "attributes": {
        "RestaurantsTakeOut": true,
        "BusinessParking": {
            "garage": false,
            "street": true,
            "validated": false,
            "lot": false,
            "valet": false
        }
    },
    "categories": [
        "Mexican",
        "Burgers",
        "Gastropubs"
    ],
    "hours": {
        "Monday": "10:00-21:00",
        "Tuesday": "10:00-21:00",
        "Friday": "10:00-21:00",
        "Wednesday": "10:00-21:00",
        "Thursday": "10:00-21:00",
        "Sunday": "11:00-18:00",
        "Saturday": "10:00-21:00"
    }
}
```

#### yelp_academic_dataset_review.json

Contains full review text data including the user_id that wrote the review and the business_id the review is written for.

```json
{
    "review_id": "zdSx_SD6obEhz9VrW9uAWA",
    "user_id": "Ha3iJu77CxlrFm-vQRs_8g",
    "business_id": "tnhfDv5Il8EaGSXZGiuQGg",
    "stars": 4,
    "date": "2016-03-09",
    "text": "Great place to hang out after work: the prices are decent, and the ambience is fun. It's a bit loud, but very lively. The staff is friendly, and the food is good. They have a good selection of drinks.",
    "useful": 0,
    "funny": 0,
    "cool": 0
}
```

#### yelp_academic_dataset_user.json

User data including the user's friend mapping and all the metadata associated with the user.

```json
{
    "user_id": "Ha3iJu77CxlrFm-vQRs_8g",
    "name": "Sebastien",
    "review_count": 56,
    "yelping_since": "2011-01-01",
    "friends": [
        "wqoXYLWmpkEH0YvTmHBsJQ",
        "KUXLLiJGrjtSsapmxmpvTA",
        "6e9rJKQC3n0RSKyHLViL-Q"
    ],
    "useful": 21,
    "funny": 88,
    "cool": 15,
    "fans": 1032,
    "elite": [
        2012,
        2013
    ],
    "average_stars": 4.31,
    "compliment_hot": 339,
    "compliment_more": 668,
    "compliment_profile": 42,
    "compliment_cute": 62,
    "compliment_list": 37,
    "compliment_note": 356,
    "compliment_plain": 68,
    "compliment_cool": 91,
    "compliment_funny": 99,
    "compliment_writer": 95,
    "compliment_photos": 50
}
```

### Demographics of U.S. Cities

I obtained demogrpahic data on cities from [Open Data Soft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/), which obtained their dataset from the U.S. Census Bureau for 2015. "This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000." The U.S. Census Bureau data is open and available to the public.

Here's an example row of data:

```json
{
    "city": "Newark",
    "state": "New Jersey",
    "median_age": 34.6,
    "male_population": 138040,
    "female_population": 143873,
    "total_population": 281913,
    "number_of_veterans": 5829,
    "foreign_born": 86253,
    "average_household_size": 2.73,
    "state_code": "NJ",
    "race": "White",
    "count": 76402
}
```

### Tax Climate

I obatined tax climate data of each U.S. state from the [Tax Foundation](https://taxfoundation.org/2021-state-business-tax-climate-index/) by writing a simple Python script (`get_business_tax_climate_data.py`). The Tax Foundation distills many complex tax considerations into a simple to understand index which ranks each state's tax system.

Here's an example row of data:

```json
{
    "state": "Alabama",
    "overall_rank": 41,
    "corporate_tax_rank": 23,
    "individual_income_tax_rank": 30,
    "sales_tax_rank": 50,
    "property_tax_rank": 19,
    "unemployment_insurance_tax_rank": 14
}
```

## Data Model Definition

![Project ERD](project_erd.png)


## ETL Process

![ETL Process](airflow_etl.png)

The DAG takes the following steps:

1. Starts Redshift Cluster and creates an IAM role for the cluster to interact with S3.
2. Adds an AWS and Redshift connection to Airflow.
3. Drops tables if they exist (this would only be True if the Redshift cluster existed before the DAG start).
4. Creates tables for each data source and summary tables.
5. Copy data from S3 to Redshift tables.
6. Perform data quality checks on newly copied data.
7. Insert data to summary tables.
8. Unload data from Redshift to S3.
9. Shutdown Redshift Cluster and delete IAM role.

## How to use this project

If you'd like to use this project yourself, I first hope that the explanation of the data, data model, and ETL process I used are helpful for understanding the following steps to run the ETL pipeline created here. *Note: The following steps assume you already have [Apache Airflow](https://airflow.apache.org/) installed and are familiar with how to create data pipelines with Airflow and AWS.*

1. Download/Fork the repository.
2. Point `AIRFLOW_HOME` to the correct root folder.
3. Create an airflow admin [user](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html#create_repeat1) and [db](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html#init).
4. Edit `airflow.cfg` to ensure the following values are pointing to the right folder:
    - dags_folder
    - sql_alchemy_conn
    - plugins_folder
    - base_log_folder
    - dag_processor_manager_log_location
    - child_process_log_directory
5. Create a `dwh.cfg` file at the root directory (same as airflow.cfg and dags folder). Copy/paste and update the file with the following:
```txt
[AWS]
KEY=<AWS-IAM-Admin-Key>
SECRET=<AWS-IAM-Admin-Secret>
S3_BUCKET=<Name-of-S3-bucket>

[DWH]
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=4
DWH_NODE_TYPE=dc2.large
DWH_IAM_ROLE_NAME=dwhRole
DWH_CLUSTER_IDENTIFIER=dwhCluster
DWH_DB=dwh
DWH_DB_USER=dwhuser
DWH_DB_PASSWORD=<DB-Password>
DWH_PORT=5439
```
6. Add [raw data files](https://drive.google.com/drive/folders/1E6D0AO8mE7OUWUtrbAPCjer8wKbQWzJu?usp=sharing) to the S3 bucket names in the `dwh.cfg` file. Specifically, unzip the `raw_data.zip` file and upload them to your S3 bucket.
7. Start airflow webserver and scheduler.
8. Run the DAG.

## Concluding thoughts

### Shortcomings of the data

With the Yelp dataset, we are only getting a selection of businesses, reviews, and users for academic purposes. As a result, if the end user wanted to do an analysis of businesses and users in a particular city not in the sample dataset, they wouldn't be able to. For a more robust analysis, we would need to access all of Yelp's data.

With the demographics dataset, the data is only accurate up to 2015. Given that the population of the United States grew by about 10 million from 2015 to 2020, it's safe to say that things have changed. In addition, this dataset is limited in what it shares. For instance, the Census Bureau has several more data points than what is captured in the dataset used in this project (e.g., cities and towns with a population less than 65,000, and many demographic variables which can be found in the [Census Bureau QuickFacts for the U.S.](https://www.census.gov/quickfacts/fact/table/US/PST045221)). For a more robust analysis, we would need to get the most recent year of data -- and perhaps data for each year we care about -- as well as the additional data points the Census Bureau offers.

### Addressing other scenarios

**What if the data was increased by 100x?**

If the data was increased by 100x, a couple items would change for this project:
- The Redshift cluster should have additional nodes for greater processing.
- There may be better options within S3 that should be considered for the data.
- If the data was being streamed in, or uploaded daily, this would also change the way in which the data should be processed. For instance, it might make sense to chunk the data before copying it to Redshift.

**What if the pipelines would be run on a daily basis by 7 am every day?**

For this project, we would need to change the schedule of the DAG, set up some alerts in case the DAG ever failed, and ensure that the webserver and scheduler are running through the day (or at least around the time of the schedule + however long it takes to run the DAG).

**What if the database needed to be accessed by 100+ people?**

If the database needed to be accessed by 100+ people, there might be a better solution than using Redshift. If, for instance, many of the users needing access just needed access to summary statistics of the data, using something like Power BI or Tableau to present the data in a more user-friendly way would be easier/better for the users. If the users needing access to the data all needed access to the database to write their own SQL queries against the database, the Airflow DAG would need to be altered so that the redshift cluster is not deleted at the end of the ETL process.