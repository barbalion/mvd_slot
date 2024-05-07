import csv
import json

import requests

service_id = 10001970310  # Получение паспорта нового поколения
service_id2 = 10000101388  # Unknown
search_region = 45000000000  # OKATO: https://classifikators.ru/okato/45000000000
org_url = 'https://www.gosuslugi.ru/api/nsi/v1/dictionary/MVD_equeue_10000101388'
slots_url = 'https://www.gosuslugi.ru/api/lk/v1/equeue/agg/slots'
headers_file = "headers.txt"


def read_headers_from_file(filename):
    headers = {}
    with open(filename, 'r') as file:
        for line in file:
            name, value = line.strip().split(':', 1)
            headers[name.strip()] = value.strip()
    return headers


def read_json_from_file(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data


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
    headers = read_headers_from_file(headers_file)
    req_data = org_req(search_region)
    response = requests.post(url, json=req_data, headers=headers)
    response.raise_for_status()  # Ensure to raise an exception for HTTP error codes
    data = response.json()
    for o in data['items']:
        v = o['attributeValues']
        yield Org(v['address'], v['code'], v['SLOTPERCENT'], v['okato'])
    return data


class Org:
    def __init__(self, address, code, slotpercent, okato):
        self.address = address
        self.code = code
        self.slotpercent = slotpercent
        self.okato = okato
        self.slots = list(sorted(self.find_slots()))

    def as_dict(self):
        return {
            'address': self.address,
            'code': self.code,
            'slotpercent': self.slotpercent,
            'okato': self.okato,
            'slots': self.slots,
        }

    def find_slots(self):
        req = {"organizationId": [self.code], "serviceId": [str(service_id)], "eserviceId": str(service_id2),
               "attributes": [], "filter": None}
        response = requests.post(slots_url, json=req, headers=read_headers_from_file(headers_file))
        response.raise_for_status()
        slots_data = response.json()
        for s in slots_data['slots']:
            yield s['visitTime']


def main():
    with open('orgs_data.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(
            ['Code', 'Address', 'Slot Percentage', 'Slots count', 'Slot1', 'Slot2', 'Slot3', 'Slot4', 'Slot5'])
        for org in read_orgs():
            # Limit the slots to display only the first 5 for brevity
            row = [org.code, org.address, org.slotpercent, len(org.slots), *org.slots[0:5]]
            print(row)
            writer.writerow(row)


if __name__ == "__main__":
    main()
