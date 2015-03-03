'''
import.io client library - example code

This file is an example for integrating with import.io using the Python client library

Dependencies: Python 2.7, importio.py, latch.py (both included in client bundle)

@author: dev@import.io
@source: https://github.com/import-io/importio-client-libs/tree/master/python
'''

import datetime
import json
import csv

import importio
import latch

import local_settings


query_latch = latch.latch(5)
data_rows = []

def callback(query, message):

    global data_rows
    # Disconnect messages happen if we disconnect the client library while a query is in progress
    if message["type"] == "DISCONNECT":
        print "Query in progress when library disconnected"
        print json.dumps(message["data"], indent = 4)

    # Check the message we receive actually has some data in it
    if message["type"] == "MESSAGE":
        if "errorType" in message["data"]:
            # In this case, we received a message, but it was an error from the external service
            print "Got an error!"
            print json.dumps(message["data"], indent = 4)
        else:
            # We got a message and it was not an error, so we can process the data
            #print "retrieved data.", len(message['data']['results'])
            # Save the data we got in our dataRows variable for later
            data_rows.extend(message["data"]["results"])

    # When the query is finished, countdown the latch so the program can continue when everything is done
    if query.finished():
        query_latch.countdown()
        print 'data rows', len(data_rows)


def extract_dict_keys(dictionary, keys=[]):
    return {k:v for (k, v) in dictionary.iteritems() if k in keys}

def main():
    global query_latch
    global data_rows

    client = importio.importio(user_id=local_settings.USER_GUID, api_key=local_settings.API_KEY)
    client.connect()

    motorcycle_list_base_url = 'http://www.ebay.co.uk/sch/Motorcycles-Scooters-/422/i.html?LH_BIN=1&_sop=10&_pgn={page}&_skc={offset}&rt=nc'
    motorcycle_list_page = 0
    motorcycle_list_offset = 0
    max_list_pages = 5
    list_tries = 0

    day_yesterday = datetime.date.fromordinal(datetime.date.today().toordinal()-1)
    date_yesterday = day_yesterday.strftime("%d-%b")

    print 'Querying listings...'
    while list_tries < max_list_pages:
        client.query({
                "input":{
                    "webpage/url": motorcycle_list_base_url.format(**{
                        'page': motorcycle_list_page,
                        'offset': motorcycle_list_offset
                        })
                },
                "connectorGuids": ["e7c93baa-5482-4e6a-b2a4-7e37fc74216c"],
            }, callback)

        motorcycle_list_page += 1
        motorcycle_list_offset += 50
        list_tries += 1
    query_latch.await()


    yesterday_motorcycles = [motorcycle for motorcycle in data_rows if motorcycle['date_posted'].startswith(date_yesterday)]
    posting_count = len(yesterday_motorcycles)
    print 'New "Buy it now" motorcycles posted yesterday:', posting_count
    data_rows[:] = []

    if yesterday_motorcycles:
        print 'Querying seller names...',
        query_latch = latch.latch(posting_count)
        for motorcycle in yesterday_motorcycles:
            client.query({
                    "input":{
                        "webpage/url": motorcycle['detail_link'],
                    },
                    "connectorGuids": ["9d68ea19-5be3-4852-b338-e79ff8c745c2"],
                }, callback)
        query_latch.await()
        sellers = data_rows
        for motorcycle, seller in zip(yesterday_motorcycles, sellers):
            #motorcycle['title'] = motorcycle['title'].decode('utf-8')
            motorcycle['seller_name'] = seller['seller_name'].decode('utf-8')

        # only keeping useful keys
        yesterday_motorcycles_cleaned = [extract_dict_keys(m, keys=['title', 'price', 'seller_name']) for m in yesterday_motorcycles]

        with open('motorcycles_{day}.csv'.format(day=date_yesterday), 'wb') as f:
            dict_writer = csv.DictWriter(f, fieldnames=['title', 'price', 'seller_name'])
            dict_writer.writeheader()
            dict_writer.writerows(yesterday_motorcycles_cleaned)



    # It is best practice to disconnect when you are finished sending queries and getting data - it allows us to
    # clean up resources on the client and the server


    client.disconnect()
    print "Done."

if __name__ == '__main__':
    main()
