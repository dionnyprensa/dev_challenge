# Challenge 2
[In this URL](https://drive.google.com/drive/folders/18cIw7TWMCrrN6MgfrKmD4IrSWsyltjfx?usp=sharing) you’ll find 4 csv’s, each csv is a snapshot of the tables deposit, withdrawal, event and user with historic information. As a data engineer you need to provide master data for our downstream users Business Intelligence, Machine Learning, Experimentation and Marketing. These teams will use the data for multiple purposes, the tables and data model you’ll create should help them answer questions like the following:
- How many users were active on a given day (they made a deposit or withdrawal)
- Identify users haven't made a deposit
- Identify on a given day which users have made more than 5 deposits historically
- When was the last time a user a user made a login
- How many times a user has made a login between two dates
- Number of unique currencies deposited on a given day
- Number of unique currencies withdrew on a given day
- Total amount deposited of a given currency on a given day

Details:
- Use the data modeling technique that you consider appropriate for the use cases mentioned above.
- Code your ETL and write the output tables in CSVs files, one per table.
- Create at least 4 queries that will answer 4 different use cases with your solution and store it on an sql or txt file

Deliverables:
- Image with the ERD of the data model
- Python 3 source code with your solution
- CSVs with the output tables
- SQL or TXT file with the queries that will answer at least 3 cases
- Read.me file explaining what modeling techniques did you use, why did you choose them and what would be potential downside of this approach

Bonus:
- ETL process daily batches
- Main script orchestrating the pipeline or the use airflow
- Implement Unit testing

----------

## Solution

### Simulate a Data warehouse
I improved the design using a star model for the Data Warehouse. I decided to simplify the existing files, which resulted in these tables: Users, Events, Interfaces, Statuses, Currencies, Deposit, Withdrawals, and Event.

Even though the file structure wasn't complicated, it wasn't very efficient for combining data. So, I chose to create new tables and move repetitive filter values to smaller tables, taking advantage of how queries are processed.

This strategy shifts the filter workload (WHERE) to a smaller set of data, and at the same time, uses this set to filter the other tables. For example:

```SQL
SELECT
  users.user_sk
  , users.user_id
  , MAX(DATE(events.event_timestamp)) AS Last_Login
FROM public."DimUsers" AS users
INNER JOIN public."FactEvents" AS events
  ON users.user_sk = events.user_sk

INNER JOIN public."DimEvents" dimEvents
  ON events.event_dim_sk = dimEvents.event_sk

WHERE
  dimEvents.event_name IN ('login', 'login_api')
AND users.user_id = '0007cda84fafdcf42f96c4f4adb7f8ce'
GROUP BY users.user_sk
```

The execution order is:
```SQL
FROM public."DimUsers" AS users

-- Join all datasets
INNER JOIN public."FactEvents" AS events
  ON users.user_sk = events.user_sk

INNER JOIN public."DimEvents" dimEvents
  ON events.event_dim_sk = dimEvents.event_sk

-- Filters (Hard work)
WHERE
  dimEvents.event_name IN ('login', 'login_api')
AND users.user_id = '0007cda84fafdcf42f96c4f4adb7f8ce'

GROUP BY users.user_sk

-- And fetch columns
SELECT
  users.user_sk
  , users.user_id
  , MAX(DATE(events.event_timestamp)) AS Last_Login
```

----------

### Advantages:

The main advantage of this design is that it initially focuses on faster queries, making it easier for non-technical users to understand. It's also quite useful for quickly developing interactive visualizations because you don't have to go through all the large tables to find different points of view (dimensions) of the events (facts).


### Disadvantages:

Queries may become slightly longer due to the addition of the new tables: Events (event names), Interfaces, Statuses, and Currencies.
Another drawback is the need for extra care in keeping the areas separate. Raw data versus processed data (without duplicates and with the correct data type).
All of this boils down to the maintenance of the transformation and loading code.

### Out files

![ERD]()

![ERD with stage]()

[Output files]()

[Queries]()

----------

### Setup
1. Install Python version 3 (and pip)
2. Install Docker
3. Copy [this files](https://drive.google.com/drive/folders/18cIw7TWMCrrN6MgfrKmD4IrSWsyltjfx?usp=sharing) to .\datalake\temp

### Usage
- Run docker-compose
  - Windows
    - `docker-compose up --build`
    - `docker-compose -f .\dwh\docker-compose.yaml up --build`
  - Unix-based
    - `docker-compose up --build`
    - `docker-compose -f ./dwh/docker-compose.yaml up --build`
- Open the airflow [dashboard](http://localhost:8080/dags/challenge_2/grid) and run the DAG `challenge_2`


Credentials:
- [Airflow](http://localhost:8080)
  - user: `challenge`
  - pass: `challenge`
- [PgAdmin](http://localhost:16543/)
  - user: `challenge@gmail.com`
  - pass: `challenge`
  - database_host: `host.docker.internal` or `localhost`
  - database_name: `challenge`
