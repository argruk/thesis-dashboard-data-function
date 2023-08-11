import json
import os
from base64 import b64encode
from json import JSONDecodeError
from multiprocessing import Pool

import requests
import pandas as pd
import time
import pika
import sys
import time

f = open('settings.json')
credentials = json.load(f)
f.close()

username = credentials['USERNAME']
password = credentials['PASSWORD']
tenant_url = credentials['TENANT_URL']


class CumulocityFetcher:
    def __init__(self, username_l, password_l, platform):
        local_credentials = b64encode(bytes(username_l + ':' + password_l, "utf-8")).decode("ascii")
        self.headers = {'Authorization': 'Basic %s' % local_credentials}
        self.base_url = platform

    def retrieve_all(self):
        resp = requests \
            .request("GET", self.__url("/inventory/binaries"),
                     headers=self.headers)
        print(resp.json())

    def retrieve_created_csv(self, csv_file_id, name):
        resp = requests \
            .request("GET", self.__url(f"/inventory/binaries/{csv_file_id}"),
                     headers=self.__add_accept_header(self.headers))

        while resp.status_code == 404:
            time.sleep(3)
            resp = requests \
                .request("GET", self.__url(f"/inventory/binaries/{csv_file_id}"),
                         headers=self.__add_accept_header(self.headers))
            print("Retry...")

        return resp.content

    def create_csv_with_ranged_measurements(self, date_from, date_to, mt):
        if mt is None:
            what = requests \
                .request("GET", self.__url("/measurement/measurements",
                                           "withTotalPages=true",
                                           "pageSize=2000",
                                           f"dateFrom={date_from}",
                                           f"dateTo={date_to}"), headers=self.__add_accept_header(self.headers))
        else:
            what = requests \
                .request("GET", self.__url("/measurement/measurements",
                                           "withTotalPages=true",
                                           f"valueFragmentType={mt}",
                                           "pageSize=2000",
                                           f"dateFrom={date_from}",
                                           f"dateTo={date_to}"), headers=self.__add_accept_header(self.headers))
        try:
            return False, what.json()
        except JSONDecodeError:
            return True, what.content

    def create_data_chunks(self, date_from, date_to, name, mt=None):
        if mt is not None:
            resp = requests \
                .request("GET", self.__url("/measurement/measurements",
                                           "withTotalPages=true",
                                           "pageSize=2000",
                                           f"valueFragmentType={mt}",
                                           f"dateFrom={date_from}",
                                           f"dateTo={date_to}"), headers=self.headers) \
                .json()

        else:
            resp = requests \
                .request("GET", self.__url("/measurement/measurements",
                                           "withTotalPages=true",
                                           "pageSize=2000",
                                           f"dateFrom={date_from}",
                                           f"dateTo={date_to}"), headers=self.headers) \
                .json()

        total_pages = int(resp['statistics']['totalPages'])

        total_records = total_pages * 2000

        page_i = 1
        pages = []
        page_difference = 100_000 // 2000

        if total_pages == 1:
            is_csv, resp = self.create_csv_with_ranged_measurements(date_from, date_to, mt)
            if is_csv:
                save_to_csv_file(resp, f"{name}-{0}")
            else:
                binary = self.retrieve_created_csv(resp['id'], name)
                save_to_csv_file(binary, f"{name}-{0}")
            print(f"Saved as {name}-{0}")
        else:
            process_pool_args = []

            while page_i < total_pages:
                pages.append(page_i)
                page_i += page_difference

            pages.append(total_pages)

            dates = []

            for i in range(len(pages)):
                if mt is not None:
                    temp_resp = requests \
                        .request("GET", self.__url("/measurement/measurements",
                                                   f"currentPage={pages[i]}",
                                                   "pageSize=2000",
                                                   f"valueFragmentType={mt}",
                                                   f"dateFrom={date_from}",
                                                   f"dateTo={date_to}"), headers=self.headers) \
                        .json()
                else:
                    temp_resp = requests \
                        .request("GET", self.__url("/measurement/measurements",
                                                   f"currentPage={pages[i]}",
                                                   "pageSize=2000",
                                                   f"dateFrom={date_from}",
                                                   f"dateTo={date_to}"), headers=self.headers) \
                        .json()

                if i == len(pages) - 1:
                    dates.append(temp_resp['measurements'][len(temp_resp['measurements']) - 1]['time'])
                else:
                    dates.append(temp_resp['measurements'][0]['time'])

            for i in range(len(dates) - 1):
                process_pool_args.append((i, dates[i], dates[i + 1], mt, name))

            with Pool(len(process_pool_args)) as pool:
                pool.map(file_download_worker, process_pool_args)

            # for i in range(len(dates) - 1):
            #     is_csv, resp = self.create_csv_with_ranged_measurements(dates[i], dates[i + 1], mt)
            #
            #     if is_csv:
            #         save_to_csv_file(resp, f"{name}-{i}")
            #     else:
            #         binary = self.retrieve_created_csv(resp['id'], name)
            #         save_to_csv_file(binary, f"{name}-{i}")
            #     print(f"Saved as {name}-{i}")

            print(f"Total records found: {total_records}\n"
                  f"Total pages: {total_pages}\n"
                  f"Split pages: {pages}\n"
                  f"Date ranges: {dates}")
        return len(pages) - 1

    def merge_downloaded_files(self, filename, nr):

        final_df = pd.DataFrame(columns=['time', 'source', 'device_name', 'fragment.series', 'value', 'unit'])
        for i in range(nr):
            temp_file = pd.read_csv(f'../thesis-toolset-backend/SavedDatasets/{filename}-{i}.csv')
            final_df = final_df.append(temp_file, ignore_index=True)

        final_df.to_csv(f'../thesis-toolset-backend/SavedDatasets/{filename}.csv', index=False)

        for i in range(nr):
            os.remove(f'../thesis-toolset-backend/SavedDatasets/{filename}-{i}.csv')

    def get_all_measurements_ranged(self, date_from, date_to):
        measurements = []
        resp = requests \
            .request("GET", self.__url("/measurement/measurements",
                                       "withTotalPages=true",
                                       "pageSize=2000",
                                       f"dateFrom={date_from}",
                                       f"dateTo={date_to}"), headers=self.headers) \
            .json()
        current_page = 1
        while "measurements" in resp and len(resp["measurements"]) != 0:
            measurements += resp["measurements"]
            resp = requests \
                .request("GET", self.__url("/measurement/measurements",
                                           "withTotalPages=true",
                                           "pageSize=2000",
                                           f"currentPage={current_page}",
                                           f"dateFrom={date_from}",
                                           f"dateTo={date_to}"), headers=self.headers) \
                .json()
            current_page += 1
        return measurements

    def __add_accept_header(self, header_object):
        new_header_obj = header_object.copy()
        new_header_obj['Accept'] = 'text/csv'
        return new_header_obj

    def __url(self, path, *args):
        if len(args) > 0:
            final_url = f"https://{self.base_url}{path}/?"
            for arg in args:
                final_url += f"{arg}&"
            return final_url.strip('&')
        else:
            return f"https://{self.base_url}{path}"


def file_download_worker(args):
    idx, date_from, date_to, mt, name = args
    cumulocity_fetcher = CumulocityFetcher(username, password, tenant_url)

    is_csv, resp = cumulocity_fetcher.create_csv_with_ranged_measurements(date_from, date_to, mt)

    if is_csv:
        save_to_csv_file(resp, f"{name}-{idx}")
    else:
        binary = cumulocity_fetcher.retrieve_created_csv(resp['id'], name)
        save_to_csv_file(binary, f"{name}-{idx}")
    print(f"Saved as {name}-{idx}")


def save_as_csv(filename, date_from, date_to, mt=None):
    runner = CumulocityFetcher(username, password, tenant_url)

    num_of_files = runner.create_data_chunks(date_from, date_to, filename, mt)

    runner.merge_downloaded_files(filename, num_of_files)


def save_to_json_file(data, filename):
    path = "../thesis-toolset-backend/SavedDatasets"

    if not os.path.exists(path):
        os.makedirs(path)

    pd.DataFrame(data).to_json(f"../thesis-toolset-backend/SavedDatasets/{filename}.json", orient='split')
    print(f"Saved result as {filename}.json")


def save_to_csv_file(binary, filename):
    if not os.path.exists("../thesis-toolset-backend/SavedDatasets/"):
        os.makedirs("../thesis-toolset-backend/SavedDatasets/")
    with open(f"../thesis-toolset-backend/SavedDatasets/{filename}.csv", 'wb')as file:
        file.write(binary)


if __name__ == '__main__':
    runner = CumulocityFetcher(username, password, tenant_url)

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        channel.queue_declare(queue='new/dataset')


        # Chunking:
        #   * Single file size <= 200,000
        #   * Loading one by one and appending to a csv object
        #   * Finally storing csv object
        def callback(ch, method, properties, body):
            print("Job started.")
            received_obj = json.loads(body.decode())
            print(received_obj)

            start = time.time()
            if 'mt' in received_obj:
                save_as_csv(received_obj['filename'], received_obj['dateFrom'], received_obj['dateTo'],
                            received_obj['mt'])
            else:
                save_as_csv(received_obj['filename'], received_obj['dateFrom'], received_obj['dateTo'])
            end = time.time()

            print(f"{end - start} s")
            print("Job completed.")


        channel.basic_consume(queue='new/dataset', on_message_callback=callback, auto_ack=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
