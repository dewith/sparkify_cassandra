# Sparkify – Data modeling with Cassandra <!-- omit in toc -->

<!-- Add buttons here -->
[![Open in Colab](https://img.shields.io/badge/-Open%20in%20Colab-e8710a?logo=google-colab)](https://colab.research.google.com/github/dewith/sparkify_cassandra)
[![Made with Python](https://img.shields.io/badge/Made%20with-Python-black)](https://www.python.org/)
![Status](https://img.shields.io/badge/Project%20status-Completed-black)
![Last commit](https://img.shields.io/github/last-commit/dewith/sparkify_cassandra?color=black)
![License](https://img.shields.io/github/license/dewith/sparkify_cassandra?color=black)
<!-- End buttons here -->

In this project I use Apache Cassandra (with its Python driver) to create a NoSQL database and an ETL pipeline for a non-real music streaming app called Sparkify.

<details>
<summary><b>Table of content</b></summary>

- [Motivation](#motivation-)
- [Datasets](#datasets-)
- [Process](#process-)
  - [Methods used](#methods-used-)
  - [Tools](#tools-)
- [Results](#results-)
  - [Tables](#tables)
    - [Sessions DB](#sessions-db)
    - [Users DB](#users-db)
    - [Song DB](#song-db)
  - [Next steps](#next-steps-)
- [Installation](#installation-)
- [File structure](#file-structure-)
- [Contact](#contact-)

</details>

<br>

## Motivation 🎯

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. So they would like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions.

> The **objective** is then to create a NoSQL database to answer the required queries and write an ETL pipeline to transfer data from files into the tables in Cassandra.

## Datasets 💾

For this project, I worked with one dataset: `event_data`, which is he directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:

```text
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

## Process ✍

1. Creation of tables.
    - Understanding the needs for the database based on queries.
    - Choosing partition keys and clustering columns for each table.
    - Writing SQL queries for creation in a Python script.
2. Building of ETL processes.
    - Development of script to process the event files.
    - Create _insert_ statements to load processed records into relevant tables of data model
    - Checking the correct operation of the pipeline for inserting records.

### Methods used 📜

- Data modeling
- ETL pipeline building

### Tools 🧰

- Python
- Cassandra
- Pandas

## Results 📣

### Tables

A data model was developed for every need stated, one table per query.

#### Sessions DB

For the first query wanted to ask the following to the data:

> Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

The table created contains these columns:

| Name            | Data type | Column type   |
|-----------------|-----------|---------------|
| session_id      | bigint    | Partition key |
| item_in_session | int       | Partition key |
| artist_name     | text      | Column        |
| song_title      | text      | Column        |
| song_length     | float     | Column        |

Sample queries:

```sql
--Query 1
SELECT artist_name, song_title, song_length 
FROM session_library 
WHERE session_id=338 AND item_in_session=4;

--Query 2
SELECT artist_name, song_title, song_length 
FROM session_library 
WHERE session_id=42;
```

#### Users DB

For the second query they wanted to ask the following to the data:

> Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

The table created contains these columns:

| Name            | Data type | Column type       |
|-----------------|-----------|-------------------|
| user_id         | bigint    | Partition key     |
| session_id      | bigint    | Partition key     |
| item_in_session | int       | Clustering column |
| artist_name     | text      | Column            |
| song_title      | text      | Column            |
| user_first_name | text      | Column            |
| user_last_name  | text      | Column            |

Sample queries:

```sql
--Query 1
SELECT artist_name, song_title, user_first_name, user_last_name 
FROM user_library 
WHERE user_id = 10 AND session_id = 182
ORDER BY item_in_session ASC;

--Query 2
SELECT artist_name, song_title, user_first_name, user_last_name 
FROM user_library 
WHERE user_id = 80 AND session_id = 435
ORDER BY item_in_session ASC;
```

#### Song DB

And in the last query they wanted to ask the following to the data:

> Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

The table created contains these columns:

| Name            | Data type | Column type       |
|-----------------|-----------|-------------------|
| song_title      | text      | Partition key     |
| user_id         | bigint    | Clustering column |
| session_id      | bigint    | Clustering column |
| item_in_session | int       | Clustering column |
| user_first_name | text      | Column            |
| user_last_name  | text      | Column            |


Sample queries:

```sql
--Query 1
SELECT user_first_name, user_last_name 
FROM song_library 
WHERE song_title = 'All Hands Against His Own';

--Query 2
SELECT user_first_name, user_last_name 
FROM song_library 
WHERE song_title = 'Yellow'
ORDER BY user_id, session_id ASC;
```

## Installation 💻

- The code was originally developed on the **Udacity's AWS Workspace** using in JupyterLab, mainly with the libraries [Cassandra Driver](https://docs.datastax.com/en/developer/python-driver/3.24/). But it can be executed locally (assuming you already have Cassandra installed locally or in a sever) by meeting these requirements:
  - `python==3.6.3`
  - `conda==4.6.14`
  - `jupyterlab==1.0.9`
  - `cassandra-driver==3.11.0`
  - `pandas==0.23.3`

## File structure 📓

- **`create_tables.py`** drops and creates the tables in the keyspace.

- **`etl.py`** processes files from `event_data` directory and loads them into the tables.

- **`cql_queries.py`** contains all the CQL queries, and it's used by `create_tables` and `etl.py`.

- **`etl.ipynb`** reads and processes the files in `event_data` and loads the records into the tables. This notebook was made to test the code before creating `etl.py`.

- **`test.ipynb`** have the queries to check each table in the database.

## Contact 📞

- You can visit my [**personal website**](https://dewithmiramon.com/),
- follow me on [**Twitter**](https://twitter.com/DewithMiramon/),
- connect with me on [**LinkedIn**](https://linkedin.com/in/dewithmiramon/),
- or check out the rest of my projects on my [**GitHub**](https://github.com/dewith/) profile.

[(Back to top)](#motivation-)