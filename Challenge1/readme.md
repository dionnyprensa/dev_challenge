# Challenge 1

“Markets team” needs to monitor the [bid-ask spread](https://www.investopedia.com/articles/investing/082213/how-calculate-bidask-spread.asp) from the [“order books”](https://www.investopedia.com/terms/o/order-book.asp) MXN_BTC and USD_MXN, they need to do custom analysis on the bid-ask spread and create alerts whenever the spread is bigger than 1.0%, 0.5%, 0.1% or any other custom value in one second observations.

To enable them you need to create a process that gets the order books each second and extract the necessary values to power the analysis. The process should store a file every 10 min with all the observations gathered (600 records) in that timespan. This file will be stored in the “data lake” and must be partitioned (You’ll define the relevant partitions).

Details:
- Spread = `(best_ask - best_bid) * 100 / best_ask`
- Use [bitso api](https://docs.bitso.com/bitso-api/docs/list-order-book)
- Create a record with the spread of the orderbook every second
  -The table should have the following schema (orderbook_timestamp: string, book: string, bid :float, ask: float, spread: float)
  - Ex. (“2022-04-30T04:10:26+00:00”, “btc_mxn”, 790333.41, 790961.89,0.079)
- The code must generate a directory structure that simulates the partitions in s3

## Solution

### Setup
1. Install Python version 3 (and pip)
2. Install the requirements: run pip install -r requirements.txt
3. Create/Setup the environment variables for Bitso API usage:
   - `API_KEY: API_KEY_example_value`
   - `API_SECRET: API_SECRET_example_value`
- *(optional)* If you don't want to create/setup a new environment variable, just replace the values in the main section of the `Challenge1.py`
  - API_KEY = ~~os.environ['API_KEY']~~
    - To `API_KEY = 'HARDCODED_API_KEY_VALUE'`
  - API_SECRET = ~~os.environ['API_SECRET']~~
    - To `API_SECRET = 'HARDCODED_API_SECRET_VALUE'`


### Usage
- "Modify the `OBSERVATION_FREQUENCY` constant in the main section of the `Challenge1.py` to set the observation frequency. The value must be expressed in seconds. Minutes * 60.
  - Ex. `10 minutes = 600 segundos`
- Run `python Challenge1.py`

----------

### Data Lake hierarchy
I decided to use a system (file hierarchy) in which the date and time will take priority in the physical partition. This adds value since it can save time when it's necessary to filter a particular day, regardless of the hour. It is also possible to retrieve records from a specific hour, regardless of the day.

Let's start

The full path is:
```data_lake\markets\<PAIR>\bid_ask_spread\<DATE>\<HOUR>\bid_ask_spread-<PAIR>-<DATE>-<HOUR>-part-<Incremental>.csv```

- `data_lake`: Data lake root

- `markets`: Data grouped by business characteristics or functionalities. This case, uses the market Data.

- `<PAIR>`: Market data grouped by pair
  - BTC_MXN
  - USD_MXN
  - BTC_USD
  - XRP_USD

- `bid_ask_spread`: A pair may have multiple data verticals within the market. This case uses the bid-ask-spread.

- `DATE`: This is the level that adds value by grouping the information by day and gives the ability to use quick filters. It uses the format YYYYMMDD (20230930)

- `HOUR`: In the same way that the previous level adds value, this one allows you to access a specific time on a given day or simply obtain information for all days, but at a particular hour.It uses the format HH(00, 01, 16, 22, 23)

- The file name follows the path structure: `bid_ask_spread-BTC_MXN-20230930-23-part-0.csv` where `part-0` is the incremental partition number. This enables the reading of a group of files using the specific pair and any combination of filters from the previous levels.


The hierarchy I've used to organize the data lake is based on my experience with Apache Spark and its functionality for reading files using physical partition filters and wildcards. I'm not familiar with the tools used beyond Python, but I hope to know them.
