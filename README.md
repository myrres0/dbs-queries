# DBS Assignment 1 and 2

## Assignment 1
simple queries to get data from the database

## Assignment 2
advanced queries to get data from the database


## Requirements
uvicorn
fastapi
psycopg2-binary
tzdata
pydantic-settings
datetime

## How to run
1. Create python virtual environment: `python -m venv venv`
2. Enter environment: `.\venv\Scripts\activate` for windows or `source venv/bin/activate` for linux
3. Install dependencies: `pip install -r requirements.txt`
4. Run application: `uvicorn dbs_assignment.__main__:app --reload`
