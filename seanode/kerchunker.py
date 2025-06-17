"""
"""


import os
import xarray as xr
import datetime as dt
import pandas as pd
import pathlib
import fsspec
import ujson
from kerchunk.grib2 import scan_grib
import dask.bag
import logging


logger = logging.getLogger(__name__)


def kerchunk_grib(grib_filename):
    """
    """
    # File system to write to: currently local (even if from AWS).
    fs_write = fsspec.filesystem('')
    
    # Create directory to save reference files to.
    try:
        dir_root = os.path.expanduser(os.environ['KERCHUNK_REF_DIR'])
    except KeyError:
        dir_root = os.path.expanduser('~/kerchunk_refs')
        os.environ['KERCHUNK_REF_DIR'] = dir_root
        logger.warning(f"kerchunk reference files will be saved in {dir_root}. To specify a different location, set environment variable KERCHUNK_REF_DIR. E.g., in bash: 'export KERCHUNK_REF_DIR=~/data/kerchunk_refs'")
    json_dir = pathlib.Path(dir_root) / pathlib.Path(grib_filename).parents[0]
    json_dir.mkdir(parents=True, exist_ok=True)
    
    # Get file name root for creating json file names.
    # Just the filename with out the ".grib2" suffix.
    json_name_root = pathlib.Path(grib_filename).stem

    # Create variable filter (hardcoded for now).
    var_filter = {'cfVarName': ['u10', 'v10', 'sp']}
    storage_opts = {"anon": True}

    # Check if there are existing reference files.
    # We could do some fancy checks for variable names here, but 
    # let's do that another day.
    existing_refs = fs_write.glob(str(json_dir / json_name_root) 
                                  + '_message*.json')
    if len(existing_refs) < len(var_filter['cfVarName']):
        # Create the reference files, and keep a list of their names.
        out = scan_grib('s3://' + grib_filename, 
                        storage_options=storage_opts, 
                        filter=var_filter)
        ref_filename_list = []
        # Loop over outputs  (scan_grib returns list with one reference per grib message/variable)
        for i, message in enumerate(out):
            out_file_name = f'{json_dir / json_name_root}_message{i}.json'
            ref_filename_list.append(out_file_name)
            logger.info(f'writing json file {out_file_name}')
            with fs_write.open(out_file_name, "w") as f: 
                f.write(ujson.dumps(message)) #write to file 
    else:
        ref_filename_list = existing_refs
    
    # Return the reference file name list.
    return ref_filename_list
    