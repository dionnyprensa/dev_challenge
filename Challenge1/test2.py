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


def connect_to_datalake():
    time.sleep(1)
    return 1


def build_path(book, timestamp):
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
        'data_lake',
        'markets',
        book,
        'bid_ask_spread',
        f'date_{year_month_day}',
        f'hour_{hour}',
        f"minute_{minute}"
    )

    return full_path


def build_file_name(timestamp, prefix='data-'):
    """Generate the filename to be saved

    :param timestamp: value for file name.
    :type timestamp: datetime
    """
    time = timestamp.strftime('%H_%M')

    file_name = f'{prefix}{time}'

    return file_name


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


def get_order_book(book='usd_mxn'):
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


def rename_fields_and_sort_list(order_type, orders_data):
    new_orders_data = []
    old_orders_data = orders_data.copy()

    old_orders_data.sort(
        key=lambda x: float(x['price']), reverse=True)

    for order in old_orders_data:
        new_order = order.copy()
        new_order.update(
            {
                f'{order_type}_price': float(new_order.pop("price"))
            }
        )

        new_order.update(
            {
                f'{order_type}_amount': float(new_order.pop("amount"))
            }
        )

        new_orders_data.append(new_order)

    return new_orders_data


def add_orderbook_timestamp(orders_data, timestamp):
    orders_data.update({"orderbook_timestamp": timestamp})
    return orders_data


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


def save_order_book_data(book, bid_data, ask_data, timestamp):
    """ Save the order book data to the Data Lake

    :param book: name of the order book to save.
    :type book: str

    :param bid_data: bid orders.
    :type bid_data: list

    :param ask_data: ask orders.
    :type ask_data: list

    :param timestamp: batch datetime for folder structure and file name.
    :type timestamp: datetime
    """

    folder_path = build_path(book, timestamp)
    file_name = build_file_name(timestamp)
    file_path = os.path.join(folder_path, file_name) + ".csv"

    # Rename the fields
    # bid_data_renamed = rename_fields_and_sort_list('bid', bid_data)
    # ask_data_renamed = rename_fields_and_sort_list('ask', ask_data)
    # spread = generate_spread_data(bid_data_renamed, ask_data_renamed)

    best_bid = get_best_bid(bid_data)
    best_ask = get_best_ask(ask_data)

    spread = (best_ask - best_bid) * 100 / best_ask

    # print(best_bid, best_ask, spread)

    # Add the orderbook_timestamp
    # full_order_book = add_orderbook_timestamp(full_order_book, timestamp)
    row = f'"{timestamp}","{book}",{best_bid},{best_ask},{spread}\n'
    print(row)

    # print(
    #     book, '->',
    #     'bid', len(bid_data),
    #     '- ask', len(ask_data), '->',
    #     timestamp
    # )

    os.makedirs(folder_path, exist_ok=True)
    file_headers = 'timestamp,book,bid,ask,spread\n'

    with open(file_path, 'w') as file:
        if os.path.exists(file_path):
            file.write(file_headers)

        file.write(row)


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
    bid_data = []
    ask_data = []
    timestamp = None

    # Get the order books every second
    while True:

        # Check if the data has reached the target records (requests_number)
        if requests_number >= requests_counter:
            data = get_order_book(book)

            updated_at = data['payload']['updated_at']
            sequence = data['payload']['sequence']
            bids = data['payload']['bids']
            asks = data['payload']['asks']

            updated_at__parsed = datetime.fromisoformat(updated_at)

            print(updated_at__parsed)

            bid_data.extend(bids)
            ask_data.extend(asks)

            # Get the first request date
            if requests_counter == 1:
                timestamp = updated_at__parsed

            requests_counter = requests_counter + 1
        else:

            thread_save_order_book_data = threading.Thread(
                target=save_order_book_data,
                kwargs={
                    'book': book,
                    'bid_data': bid_data.copy(),
                    'ask_data': ask_data.copy(),
                    'timestamp': timestamp
                }
            )

            thread_save_order_book_data.start()

            break  # don't break. Just for test

            requests_counter = 0
            bid_data = []
            ask_data = []
            timestamp = None

        # Sleep for 1 second (0.5 second to compensate for any delay)
        time.sleep(0.5)


def main():
    # requests_number=600
    process_order_book_data(book='usd_mxn', requests_number=1)
    # process_order_book_data('btc_mxn')

    # # Create the threads
    # thread_usd_mxn = threading.Thread(
    #     target=process_order_book_data, args=("usd_mxn",)
    # )

    # thread_btc_mxn = threading.Thread(
    #     target=process_order_book_data, args=("btc_mxn",)
    # )

    # # Start the threads
    # thread_usd_mxn.start()
    # thread_btc_mxn.start()

    # # Wait for the threads to finish
    # thread_usd_mxn.join()
    # thread_btc_mxn.join()


# Run the main function
if __name__ == '__main__':
    main()
