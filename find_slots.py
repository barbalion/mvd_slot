import csv
import json
import multiprocessing
import os
import signal
import time

import requests
from requests import JSONDecodeError

service_id = 10001970310  # Получение паспорта нового поколения
queue_id = 10000101388  # Unknown
search_region = 45000000000  # OKATO: https://classifikators.ru/okato/45000000000
# org_url = f'https://www.gosuslugi.ru/api/nsi/v1/dictionary/MVD_equeue_{queue_id}'
org_url = f'https://www.gosuslugi.ru/api/nsi/v1/dictionary/pgu_mvd_org'
slots_url = 'https://www.gosuslugi.ru/api/lk/v1/equeue/agg/slots'
headers_file = "headers.txt"

num_threads = 5
request_min_interval_sec = 0.5
num_slots_to_print = 30
block_sleep_time_sec = 5


def read_headers(filename):
    headers = {}
    with open(filename, 'r') as file:
        for line in file:
            name, value = line.strip().split(':', 1)
            headers[name.strip()] = value.strip()
    return headers


def org_req(region):
    if os.path.exists("org_request.json"):
        return json.load(open("org_request.json"))
    return {
        "filter": {"union": {"unionKind": "AND", "subs": [
            {"simple": {"attributeName": "region", "condition": "EQUALS",
                        "value": {"asString": str(region)},
                        "trueForNull": False}}, {
                "simple": {"attributeName": "department",
                           "condition": "EQUALS",
                           "value": {"asString": str(service_id)},
                           "trueForNull": False}}]}},
        "treeFiltering": "ONELEVEL", "pageNum": 1, "pageSize": 100000, "parentRefItemValue": "",
        "selectAttributes": ["*"], "tx": ""
    }


def read_orgs(url=org_url):
    print(f'Reading orgs...')
    response = requests.post(url, json=(org_req(search_region)), headers=(read_headers(headers_file)))
    response.raise_for_status()  # Ensure to raise an exception for HTTP error codes
    items = response.json()['items']
    print('Found', len(items), 'orgs.')
    for o in items:
        v = o['attributeValues']
        yield Org(
            v.get('address') or v.get('address_out'),
            v.get('code') or v.get('CODE_FRGU'),
            v.get('SLOTPERCENT'), v.get('okato'),
        )


def handler(*_):
    raise Exception('Interrupted.')


def find_slots(org, start_time=None):
    signal.signal(signal.SIGINT, handler)
    sleep_time = start_time - time.time()
    if start_time and sleep_time > 0:
        time.sleep(sleep_time)

    while True:
        try:
            req = {"organizationId": [str(org)], "serviceId": [str(service_id)], "eserviceId": str(queue_id),
                   "attributes": [], "filter": None}
            response = requests.post(slots_url, json=req, headers=read_headers(headers_file))
            response.raise_for_status()
            slots_data = response.json()
            slots = list(sorted([s['visitTime'] for s in slots_data['slots']]))
            print(f'Found {len(slots)} slots for {org}...')
            return slots
        except JSONDecodeError as e:
            print(f'We looks to be blocked "{e}"! Refresh headers! Sleeping {block_sleep_time_sec} seconds...')
            time.sleep(block_sleep_time_sec)
        except Exception as e:
            print(f'Error reading slots for {org}: {e}')
            return [f'error']


class Org:
    def __init__(self, address, code, slotpercent, okato):
        self.address = address
        self.code = code
        self.slotpercent = slotpercent
        self.okato = okato
        self.slots = []

    def get_row(self):
        return [self.code, self.address, self.slotpercent, len(self.slots), *self.slots[:num_slots_to_print]]

    def get_slot_rows(self):
        yield from [[self.code, self.address, sl] for sl in self.slots]


def write_orgs_output(orgs):
    print(f'Writing output...')
    with open('orgs_data.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(
            ['Code', 'Address', 'Slot Percentage', 'Slots count', *[f'Slot{n + 1}' for n in range(num_slots_to_print)]])
        for org in orgs:
            writer.writerow(org.get_row())


def write_slot_output(orgs):
    print(f'Writing flat output...')
    with open('orgs_data_flat.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Code', 'Address', 'Slot'])
        for org in orgs:
            writer.writerows(org.get_slot_rows())
            

def main():
    multiprocessing.freeze_support()
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    pool = multiprocessing.Pool(processes=num_threads)
    signal.signal(signal.SIGINT, original_sigint_handler)
    try:
        orgs = list(read_orgs())
        job_params = list([(o.code, time.time() + i * request_min_interval_sec) for i, o in enumerate(orgs)])
        slots = list(pool.starmap(find_slots, job_params))
        for o, s in zip(orgs, slots):
            o.slots = s
        write_orgs_output(orgs)
        write_slot_output(orgs)
        print(f'Done.')
    except (KeyboardInterrupt, SystemExit):
        pool.terminate()
        pool.join()


if __name__ == "__main__":
    main()
