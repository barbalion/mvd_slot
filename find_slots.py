import csv
import multiprocessing
import signal

import requests

service_id = 10001970310  # Получение паспорта нового поколения
queue_id = 10000101388  # Unknown
search_region = 45000000000  # OKATO: https://classifikators.ru/okato/45000000000
org_url = f'https://www.gosuslugi.ru/api/nsi/v1/dictionary/MVD_equeue_{queue_id}'
slots_url = 'https://www.gosuslugi.ru/api/lk/v1/equeue/agg/slots'
headers_file = "headers.txt"

num_threads = 30
num_slots_to_print = 30


def read_headers(filename):
    headers = {}
    with open(filename, 'r') as file:
        for line in file:
            name, value = line.strip().split(':', 1)
            headers[name.strip()] = value.strip()
    return headers


def org_req(region):
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
        yield Org(v['address'], v['code'], v['SLOTPERCENT'], v['okato'])


def find_slots(org):
    try:
        print(f'Reading slots for {org}...')
        req = {"organizationId": [str(org)], "serviceId": [str(service_id)], "eserviceId": str(queue_id),
               "attributes": [], "filter": None}
        response = requests.post(slots_url, json=req, headers=read_headers(headers_file))
        response.raise_for_status()
        slots_data = response.json()
        return list(sorted([s['visitTime'] for s in slots_data['slots']]))
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


def write_output(orgs):
    print(f'Writing output...')
    with open('orgs_data.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(
            ['Code', 'Address', 'Slot Percentage', 'Slots count', *[f'Slot{n+1}' for n in range(num_slots_to_print)]])
        for org in orgs:
            writer.writerow(org.get_row())


def main():
    multiprocessing.freeze_support()
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    pool = multiprocessing.Pool(processes=num_threads)
    signal.signal(signal.SIGINT, original_sigint_handler)
    orgs = list(read_orgs())
    slots = list(pool.map(find_slots, [o.code for o in orgs]))
    for o, s in zip(orgs, slots):
        o.slots = s
    write_output(orgs)
    print(f'Done.')


if __name__ == "__main__":
    main()
