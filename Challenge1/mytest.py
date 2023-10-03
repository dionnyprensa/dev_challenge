import os

import time

folder = os.path.join(
    os.getcwd(),
    'data_lake',
    'markets',
    'usd_mxn',
    'bid_ask_spread',
    '20231001'
)

last_file_id = 0

files = os.listdir(folder)

files = list(
    filter(lambda f:
           f.startswith(
               'spread_usd_mxn-20231001-00') and f.endswith('.csv'), files
           )
)

files = list(
    map(lambda f:
        int(f.replace('.csv', '').split('part-').pop()), files
        )
)


if files is not None and len(files) > 0:
    files = sorted(files)
    last_file_id = files.pop()

# print(files)
# print(last_file_id)
# print(last_file_id+1)

i = 1
while i <= 1000:
    print(f"{i}. Isabella")
    i = i + 1
    time.sleep(1)
