"""Data storage systems on which files/FieldSources can be accessed.

Classes
-------
DataStore
    Abstract base class.
AWSDataStore
    Public/anonymous file store on AWS.

Methods
-------
open_file
    Available in each subclass. 

"""


from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import List
import s3fs
import fsspec
import xarray
import zarr
from kerchunk.combine import MultiZarrToZarr
from seanode.kerchunker import kerchunk_nc
import logging
import traceback


logger = logging.getLogger(__name__)


class DataStore(ABC):
    """Abstract base class for DataStores.

    Do not use directly.
    
    """
    
    @abstractmethod
    def open_file(self):
        pass


class AWSDataStore(DataStore):
    """Publicly available data store on AWS.

    Attributes
    ----------
    None

    Methods
    -------
    open_file(fullpath, file_format)
        Opens a file (at fullpath, having a given file_format) from the data store.
        
    """

    def open_file(self, fullpath: str | list, file_format: str = "nc") -> xarray.Dataset:
        """Opens a file and returns a dataset.

        Note grib files are read with filters applied to the vertical levels,
        so not all variables will be returned.

        Parameters
        ----------
        fullpath
            Full path to a file, including the bucket and key,
            but not including the "s3://" prependix.
            For kerchunk file formats, this can be a list of 
            json reference files.
        file_format
            The format of the file, e.g., "nc" or "grib2".

        Returns
        -------
        xarray.Dataset
            containing data in the given file.
            
        """
        if file_format == 'nc':
            filesystem = s3fs.S3FileSystem(anon=True)
            url = f"s3://{fullpath}"
            ds = xarray.open_dataset(filesystem.open(url, 'rb'),
                                     drop_variables=['nvel'])
            return ds   

        elif file_format in ['grib', 'grib2']:
            # Cannot open grib files direct from a remote filesystem,
            # so we have to cache locally. Method from:
            # https://stackoverflow.com/questions/66229140/xarray-read-remote-grib-file-on-s3-using-cfgrib
            # TODO: work out how to handle differently if working on AWS cluster vs. another cluster or laptop.
            file = fsspec.open_local(
                'filecache::s3://' + fullpath, 
                s3={'anon': True}
            )
            # Right now these filters are hard-coded, but if 
            # different variables are needed, we could pass a  
            # list of filters based on what variables are needed.
            ds_surface = xarray.open_dataset(
                file, 
                engine="cfgrib", 
                filter_by_keys={'stepType': 'instant', 
                                'typeOfLevel': 'surface'},
                decode_timedelta=True
            ).drop_dims('surface', errors='ignore')
            ds_wind = xarray.open_dataset(
                file,
                engine="cfgrib", 
                filter_by_keys={'typeOfLevel': 'heightAboveGround', 
                                'level':10},
                decode_timedelta=True
            ).drop_dims('heightAboveGround', errors='ignore')
            return xarray.merge([ds_wind, ds_surface])

        elif file_format in ['grib_kerchunk']:
            # Combine individual references into single consolidated reference
            # Note some of these options are hard-coded for GFS. 
            # Might need to change if additional kerchunk datasets are added.
            mzz = MultiZarrToZarr(
                fullpath,
                concat_dims=['valid_time'],
                identical_dims=['latitude', 'longitude', 
                                'heightAboveGround', 'surface', 
                                'step'],
                remote_options={'skip_instance_cache': True},
                target_options={'skip_instance_cache': True}
            )
            mzz_trans = mzz.translate()
            # Open dataset as zarr object using fsspec reference file system and xarray
            fs = fsspec.filesystem("reference", fo=mzz_trans, 
                                   remote_protocol='s3', 
                                   remote_options={'anon':True,
                                                   'skip_instance_cache': True},
                                   target_options={'skip_instance_cache': True})
            store = zarr.storage.FsspecStore(fs)
            ds = xarray.open_dataset(store, engine="zarr", 
                                     backend_kwargs=dict(consolidated=False), 
                                     chunks={'valid_time':1}, 
                                     decode_timedelta=True)
            return ds.drop_vars(['step', 'surface', 'heightAboveGround'], 
                                errors='ignore')

        elif file_format in ['nc3_kerchunk', 'nc4_kerchunk']:
            json_ref = kerchunk_nc(fullpath, file_format)
            fs = fsspec.filesystem(
                "reference", 
                fo=json_ref, 
                remote_protocol='s3',
                remote_options={'anon':True,'skip_instance_cache':True},
                target_options={'skip_instance_cache': True}
            )
            store = zarr.storage.FsspecStore(fs)
            ds = xarray.open_dataset(
                store, 
                engine="zarr", 
                backend_kwargs=dict(consolidated=False), 
                chunks={'time':1}, 
                decode_timedelta=True
            )
            return ds
            
        else:
            raise ValueError(f'File format {file_format} not supported.')
            return None
        
    
