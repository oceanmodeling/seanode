"""Factory to return a data store object.

Functions
---------
get_data_store

"""


import sys
from seanode.request_options import DataStoreOptions
from seanode.data_stores import AWSDataStore, DataStore


def get_data_store(store_name: DataStoreOptions) -> DataStore:
    """Return a DataStore object given a data store name."""
    if store_name == DataStoreOptions.AWS:
        return AWSDataStore()
    else:
        raise ValueError(f'data store {store_name} not recognized.')