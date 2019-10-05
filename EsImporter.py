import json
from typing import List
from elasticsearch import Elasticsearch 
import requests

base_url = 'https://apisandbox.openbankproject.com'
consumer_key = '0z4lzvxpbl2s5funifdai1qevqenhi2hwirje5kl'
user_name = 'baishuang'
password = 'Baishuang`12'
es = Elasticsearch(['localhost:9200'])

index_at_bank = 'firehose_at_bank'
index_customers = 'firehose_customers'
index_transactions = 'firehose_transactions'


def get_token():
    authorization = 'DirectLogin username="{0}",password="{1}",consumer_key="{2}"'.format(user_name, password, consumer_key)
    headers = {'Content-Type': 'application/json', 'Authorization': authorization}
    
    response = requests.post(url='{}/my/logins/direct'.format(base_url), headers=headers)
    if response.status_code != requests.codes.ok:
        response.raise_for_status()
    body = response.content
    token = json.loads(body)["token"]
    return token


def get_bank_ids():
    response = requests.get('{}/obp/v4.0.0/banks'.format(base_url))
    if response.status_code != requests.codes.ok:
        response.raise_for_status()

    body = response.content
    banks = json.loads(body)["banks"]
    bank_ids: List[str] = [x['id'] for x in banks]
    return bank_ids


auth_header = {
    'Content-Type': 'application/json',
    'Authorization': 'DirectLogin token="{}"'.format(get_token())
}


def get_firehose_at_bank(bank_id):
    response = requests.get(url='{}/obp/v4.0.0/banks/{}/firehose/accounts/views/owner'.format(base_url, bank_id),
                            headers=auth_header)
    if response.status_code != requests.codes.ok:
        response.raise_for_status()

    body = response.content
    result = json.loads(body)["accounts"]
    print('fetch bank_id={} firehose at bank success'.format(bank_id))
    return result


def get_firehose_customers(bank_id):
    response = requests.get(url='{}/obp/v4.0.0/banks/{}/firehose/customers'.format(base_url, bank_id),
                            headers=auth_header)
    if response.status_code != requests.codes.ok:
        response.raise_for_status()

    body = response.content
    result = json.loads(body)["customers"]
    print('fetch bank_id={} firehose customers success'.format(bank_id))
    return result


def get_firehose_transactions(bank_id):
    # todo the url have a not supplied parameter: ACCOUNT_ID
    response = requests.get(
        url='{}/obp/v4.0.0/banks/{}/firehose/accounts/ACCOUNT_ID/views/owner/transactions'.format(base_url, bank_id),
        headers=auth_header)
    if response.status_code != requests.codes.ok:
        response.raise_for_status()

    body = response.content
    result = json.loads(body)["transactions"]
    print('fetch bank_id={} firehose transactions success'.format(bank_id))
    return result


def send_to_es(index_name, content, id_name='id'):
    if len(content) == 0:
        return

    values = []
    for account in content:
        values.append({"index": {"_id": account[id_name]}})
        values.append(account)
    es.bulk(index=index_name, doc_type='_doc', body=values)


if __name__ == '__main__':
    at_bank_count = set()
    customers_count = set()
    transactions_count = set()

    for b_id in get_bank_ids():
        accounts = get_firehose_at_bank(b_id)
        at_bank_count.update(map(lambda x: x['id'], accounts))
        send_to_es(index_at_bank, accounts)

        customers = get_firehose_customers(b_id)
        customers_count.update(map(lambda x: x['customer_id'], customers))
        send_to_es(index_customers, customers, 'customer_id')
        # todo when fix the method get_firehose_transactions, un-comment the follow lines
        # transactions = get_firehose_transactions(b_id)
        # transactions_count.update(map(lambda x: x['id'], transactions))
        # send_to_es(index_transactions, transactions)

    print('at bank insert count {}'.format(len(at_bank_count)))
    print('customers insert count {}'.format(len(customers_count)))
    print('transactions insert count {}'.format(len(transactions_count)))
