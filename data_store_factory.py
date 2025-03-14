"""
"""


import sys
from request_options import DataStoreOptions
import data_stores


def get_data_store(store_name):
    """
    """
    if store_name == DataStoreOptions.AWS:
        return data_stores.AWSDataStore()
    else:
        sys.exit(f'data store {store_name} not recognized.')