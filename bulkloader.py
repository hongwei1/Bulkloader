#!/usr/bin/env python3
"""
Bulkloader to load bank data into Elasticsearch

Requirements: pip3 install elasticsearch

The bigger the BULK_SIZE the faster the import.
But if only one item in the bulk fails, all items afterwards fail to index!
As a remedy, the script currently removes the offending items from the bulk
and resends it.

Despite a timeout being set to e.g. 120 seconds when instantiating the
Elasticsearch client, the server times out when a bulk import has millions
of items, e.g. 5 000 000.
Therefore, the loader can be configued to sleep for TIMEOUT seconds after
each sent bulk. Advisable for the large transacion datasets.
"""

import copy
import json
import logging
import os
import time
from collections import OrderedDict
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from io import open


INDEX_PREFIX = 'test-'
TYPE = 'default'
BULK_SIZE = 10000
DATA_DIR = os.path.abspath('./data')
LOG_DIR = os.path.abspath('./data/log')
TIMEOUT = 1
SEPARATOR = ','

RUNNER_CONFIG = [
#    (
#        'markets',
#        'data_sandbox_refcli_marche_201704.txt',
#        'map-refcli_marche.json'
#    ),
#    (
#        'taxes',
#        'data_sandbox_refcli_impots_201704.txt',
#        'map-refcli_impots.json'
#    ),
#    (
#        'charges',
#        'data_sandbox_refcli_charges_201704.txt',
#        'map-refcli_charges.json'
#    ),
#    (
#        'sme-client',
#        'data_sandbox_refclient_pro_201702.txt',
#        'map-refclient_pro.json'
#    ),
#    (
#        'revenues',
#        'data_sandbox_refcli_revenus_201704.txt',
#        'map-refcli_revenus.json'
#    ),
    (
        'accounts',
        'accounts.txt',
        os.path.abspath('./data/accounts.json')
    ),
#    (
#        'assets',
#        'data_sandbox_refcli_patrimoines_201704.txt',
#        'map-refcli_patrimoines.json'
#    ),
#    (
#        'individual-clients',
#        'data_sandbox_refclient_201704.txt',
#        'map-refclient.json'
#    ),
#    (
#        'sme-contracts',
#        'data_sandbox_contrats_pro_201702.txt',
#        'map-contrats_pro.json'
#    ),
#    (
#        'contracts',
#        'data_sandbox_contrats_201704.txt',
#        'map-contrats.json'
#    ),
#    (
#        'sme-transactions',
#        'data_sandbox_transactions_pro_201702.txt',
#        'map-transactions_pro.json',
#        True
#    ),
#    (
#        'transactions',
#        'data_sandbox_transactions_201704.txt',
#        'map-transactions.json',
#        True
#    ),
]


class BulkLoader(object):
    def init_logging(self):
        # Remove all handlers associated with the root logger object.
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        now = datetime.now().strftime('%Y%m%d%H%M%S')
        log_name = '{}.{}.bulkloader.log'.format(self.index, now)
        log_name = os.path.join(LOG_DIR, log_name)
        logging.basicConfig(filename=log_name, level=logging.WARN)
        self.logger = logging.getLogger(
            '{}.bulkloader'.format(self.index))
        self.logger.warn('Import index {} from {}'.format(
            self.index, self.file_data))

    def __init__(self, index_postfix, file_data, file_map, sleep=False):
        self.index = '{}{}'.format(INDEX_PREFIX, index_postfix)
        self.file_data = file_data
        self.file_map = file_map
        self.sleep = sleep
        self.bulk_size = BULK_SIZE
        self.client = Elasticsearch()
        self.action_template = {
            '_op_type': 'index',
            '_index': self.index,
            '_type': TYPE,
            '_id': None,
        }
        self.action_id = 0
        self.fields = []
        self.init_logging()

    def build_action(self, line):
        action = copy.deepcopy(self.action_template)
        line_split = line.strip().split(SEPARATOR)
        # Need to define _id to remove action if indexing failed
        action['_id'] = str(self.action_id)
        self.action_id += 1
        i = 0
        for field in self.fields:
            action[field] = line_split[i]
            # Empty dates need to be set to null for ES
            if not action[field] and field.find('_date') > 0:
                action[field] = None
            i += 1
        return action

    # noinspection PyInterpreter
    def send_bulk(self, actions):
        len_actions = len(actions)
        try:
            msg = 'Sending {} actions ...'.format(len_actions)
            self.logger.warn(msg)
            response = bulk(self.client, actions)
        except BulkIndexError as err:
            self.logger.error(err)
            # A few actions contained illegal data
            if len_actions == 1:
                return
            self.logger.warn('Resending reduced bulk ...')
            # Remove offending items by _id
            # TODO: remove all offenders in one walk
            for error in err.errors:
                id = error['index']['_id']
                actions = [d for d in actions if d['_id'] != id]
            self.send_bulk(actions)
        if self.sleep:
            time.sleep(TIMEOUT)

    def collect_and_send(self, data):
        count = 0
        actions = []
        for line in data:
            if count == self.bulk_size:
                self.send_bulk(actions)
                count = 0
                actions = []
            else:
                action = self.build_action(line)
                actions.append(action)
                count += 1
        # send remaining actions
        self.send_bulk(actions)

    def run(self):
        with open(self.file_map, 'r') as map:
            self.fields = json.load(map, object_pairs_hook=OrderedDict).keys()
        file_data = os.path.join(DATA_DIR, self.file_data)
        with open(file_data, 'rt', encoding="utf-8") as data:
            # skip first line (header)
            #data.readline()
            self.collect_and_send(data)


if __name__ == '__main__':
    for config in RUNNER_CONFIG:
        BulkLoader(*config).run()
