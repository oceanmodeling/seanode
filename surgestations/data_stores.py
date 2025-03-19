"""
"""


from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import List
import datetime
import s3fs
import xarray


class DataStore(ABC):
    """
    """
    
    @abstractmethod
    def open_file(self):
        pass


class AWSDataStore(DataStore):
    """
    """

    filesystem = s3fs.S3FileSystem(anon=True)

    def open_file(self, fullpath, format="nc"):
        if format == "nc":
            url = f"s3://{fullpath}"
            ds = xarray.open_dataset(self.filesystem.open(url, 'rb'), 
                                     drop_variables=['nvel'])
            return ds 
        else:
            print(f'File format {format} not supported.')
            return None
        
    