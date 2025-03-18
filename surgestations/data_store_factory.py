"""
"""


import sys
from surgestations.request_options import DataStoreOptions
from surgestations.data_stores import AWSDataStore


def get_data_store(store_name):
    """
    """
    if store_name == DataStoreOptions.AWS:
        return AWSDataStore()
    else:
        sys.exit(f'data store {store_name} not recognized.')