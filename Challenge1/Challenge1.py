import hashlib
import hmac
import itertools
import json
import os
import threading
import time
from datetime import datetime

import requests

API_VERSION = 'api/v3'
BASE_URL = f'https://sandbox.bitso.com'
API_KEY = os.environ['API_KEY']
API_SECRET = os.environ['API_SECRET']
OBSERVATION_FREQUENCY = 600


def generate_path_to_folder(book, timestamp):
    """Generate the path where the files will be saved

    :param book: name of the order book to save.
    :type book: str

    :param timestamp: value for create the folder structure.
    :type timestamp: datetime
    """

    # to get the current working directory
    directory = os.getcwd()

    # Get the current date and time for the folder structure
    year_month_day = timestamp.strftime('%Y%m%d')
    hour = timestamp.strftime('%H')
    minute = timestamp.strftime('%M')

    # Join values and build de path
    full_path = os.path.join(
        directory,
        'data_lake',
        'markets',
        book,
        'bid_ask_spread',
        f'{year_month_day}',
        f'{hour}'
    )

    # full_path = os.path.join(
    #     'data_lake',
    #     'markets',
    #     book,
    #     'bid_ask_spread',
    #     f'date_{year_month_day}',
    #     f'hour_{hour}',
    #     f"minute_{minute}"
    # )

    return full_path


def generate_file_name(timestamp, prefix='spread-'):
    """Generate the filename to be saved

    :param timestamp: value for file name.
    :type timestamp: datetime

    :param prefix: file name prefix
    :type prefix: str
    """
    _timestamp = timestamp.strftime('%Y%m%d-%H')
    # time = timestamp.strftime('%H_%M')

    file_name = f'{prefix}{_timestamp}-part-'

    return file_name


def get_last_file_partition(folder, file_name):
    """ Get the last partition file number

    :param folder: URL resource part.
    :type folder: str

    :param file_name: URL resource part.
    :type file_name: str
    """
    last_partition = None

    files = os.listdir(folder)

    files = list(
        filter(lambda f:
               f.startswith(file_name) and f.endswith('.csv'), files
               )
    )

    files = list(
        map(lambda f:
            int(f.replace('.csv', '').split('part-').pop()), files
            )
    )

    if files is not None and len(files) > 0:
        files = sorted(files)
        last_partition = files.pop()

    return last_partition


def sign_request(request_endpoint):
    """Create the request signature for the Authorization header request

    :param request_endpoint: URL resource part.
        Examples: ``open_orders`` or ``order_book?book=btc_mxn``
    :type request_endpoint: str
    """

    secs = int(time.time())
    dnonce = secs * 1000
    http_method = 'GET'
    json_payload = ''
    request_path = f'/{API_VERSION}/request_endpoint'

    data = f'{dnonce}{http_method}{request_path}{json_payload}'

    signature = hmac.new(
        API_SECRET.encode(),
        data.encode(),
        hashlib.sha256,
    ).hexdigest()

    auth_header = f'Bitso {API_KEY}:{dnonce}:{signature}'
    return {'Authorization': auth_header}


def fetch_order_book(book='usd_mxn'):
    """Fetch the order book data from the API

    :param book: name of the order book to search.
        Examples: ``usd_mxn`` or ``btc_mxn``.
        Default: ``usd_mxn``
    :type book: str
    """

    url_endpoint = f'order_book/?book={book}'
    headers = sign_request(url_endpoint)

    url = f'{BASE_URL}/{API_VERSION}/{url_endpoint}'

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = json.loads(response.content)
        return data
    else:
        raise Exception(f'Error getting order books: {response.status_code}')


def get_best_bid(bid_data):
    """ get the highest price that a buyer is willing to pay

    :param bid_data: bid orders.
    :type bid_data: list
    """
    best_bid = None
    for order in bid_data:
        price = float(order["price"])
        if best_bid is None or price > best_bid:
            best_bid = price
    return best_bid


def get_best_ask(ask_data):
    """ get the lower price that a seller is willing to sell

    :param ask_data: ask orders.
    :type ask_data: list
    """
    best_ask = None
    for order in ask_data:
        price = float(order["price"])
        if best_ask is None or price < best_ask:
            best_ask = price
    return best_ask


def generate_spread_data(bid_data, ask_data):
    """ Generate the spread

    :param bid_data: bid orders.
    :type bid_data: list

    :param ask_data: ask orders.
    :type ask_data: list
    """
    best_bid = get_best_bid(bid_data)
    best_ask = get_best_ask(ask_data)

    spread = (best_ask - best_bid) * 100 / best_ask


def save_order_book_data(book, bid_data, ask_data, timestamp, new_partition):
    """ Save the order book data to the Data Lake

    :param book: name of the order book to save.
    :type book: str

    :param bid_data: bid orders.
    :type bid_data: list

    :param ask_data: ask orders.
    :type ask_data: list

    :param timestamp: batch datetime for folder structure and file name.
    :type timestamp: datetime

    :param new_partition: create a new file partition or append the data.
    :type new_partition: bool
    """

    path_to_folder = generate_path_to_folder(book, timestamp)

    os.makedirs(path_to_folder, exist_ok=True)

    file_name = generate_file_name(timestamp, f'bid_ask_spread-{book}-')

    last_partition = get_last_file_partition(path_to_folder, file_name)

    # Start partition in 0 if not exists any partitions
    if new_partition and last_partition is None:
        file_partition = 0

    # Start new partitions in 1 if directory has partition 0
    if new_partition and last_partition is not None and last_partition == 0:
        file_partition = 1

    # Start new partitions in last+1 if directory has partitions
    elif new_partition and last_partition is not None and last_partition > 0:
        file_partition = last_partition + 1

    # If not exists any partitions:
    # Start new partitions in 0 or
    # Use the partition 0
    elif last_partition is None:
        file_partition = 0

    # Force to use the current partition
    # Just append the data
    else:
        file_partition = last_partition

    full_file_name = file_name + str(file_partition) + ".csv"

    path_to_file = os.path.join(path_to_folder, full_file_name)

    file_headers = 'timestamp,book,bid,ask,spread'

    best_bid = get_best_bid(bid_data)
    best_ask = get_best_ask(ask_data)

    spread = (best_ask - best_bid) * 100 / best_ask

    file_content = f'"{timestamp}","{book}",{best_bid},{best_ask},{spread}\n'

    # Partition 0 may be interrupted
    # Overwrite file if new_partiion is True
    # w: write/create (or overwrite if file exists)
    # a: append (or create if file exists)
    file_open_option = "w" if new_partition else 'a'

    with open(path_to_file, file_open_option) as file:
        if new_partition:
            file.write(file_headers+'\n')

        file.write(file_content)

    print(
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        f'- Processed {book} spread data. File:',
        full_file_name)


def process_order_book_data(book='usd_mxn', requests_number=1):
    """ Fetch and save the order book data to the Data Lake

    :param book: name of the order book to process.
        Examples: ``usd_mxn`` or ``btc_mxn``.
        Default: ``usd_mxn``
    :type book: str

    :param requests_number: Maximum number of requests.
        Default: 1
    :type requests_number: int
    """
    requests_counter = 1
    timestamp = None

    _temp_counter = 1
    _temp_should_stop = False

    # print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    #       f'- Process {book} spread data')

    # Get the order books every second
    # while True: # uncomment when delete the _temp_*
    while _temp_should_stop is False:
        # Check if the data has reached the target records (requests_number)
        if requests_number > requests_counter:
            data = fetch_order_book(book)

            updated_at = data['payload']['updated_at']
            sequence = data['payload']['sequence']
            bids = data['payload']['bids']
            asks = data['payload']['asks']

            timestamp = datetime.fromisoformat(updated_at)

            # Use a new thread to process and save in parallel.
            thread_save_order_book_data = threading.Thread(
                target=save_order_book_data,
                kwargs={
                    'book': book,
                    'bid_data': bids.copy(),
                    'ask_data': asks.copy(),
                    'timestamp': timestamp,
                    'new_partition': requests_counter == 1
                }
            )

            thread_save_order_book_data.start()

            requests_counter = requests_counter + 1
        else:
            # break  # don't break. Just for test

            print(
                timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                f'- Processing {book} order book data'
            )

            print(f'\tVan {_temp_counter}. Faltan {12-_temp_counter}')
            _temp_should_stop = _temp_counter == 12
            _temp_counter = _temp_counter + 1

            # Clean control and util variables
            requests_counter = 0
            thread_save_order_book_data = None

        # Sleep for 1 second (0.5 second to compensate any delay)
        time.sleep(0.5)


def main():
    # # # # # process_order_book_data(book='usd_mxn', requests_number=10) # just for Test
    # # # # # process_order_book_data(book='btc_mxn', requests_number=10) # just for Test

    # while datetime.now().minute not in [0, 10, 20, 30, 40, 50]:
    while datetime.now().minute not in [0]:
        time.sleep(1)

    thread_usd_mxn = threading.Thread(
        target=process_order_book_data, kwargs={
            'book': 'usd_mxn',
            'requests_number': OBSERVATION_FREQUENCY
        }
    )
    thread_usd_mxn.start()
    thread_usd_mxn.join()

    time.sleep(60)

    thread_btc_mxn = threading.Thread(
        target=process_order_book_data, kwargs={
            'book': 'btc_mxn',
            'requests_number': OBSERVATION_FREQUENCY
        }
    )

    thread_btc_mxn.start()
    thread_btc_mxn.join()


# Run the main function
if __name__ == '__main__':
    main()
