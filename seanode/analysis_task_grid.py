"""Define a task of opening, subsetting, and transforming a grid file dataset.

A grid structured file has data variables with separate latitude and longitude
dimensions, each of which are 1-dimensional coordinates.

Classes
-------
GridAnalysisTask
    The subclass of AnalysisTask class, with methods for grid data files.
    
"""


from abc import ABC, abstractmethod
from typing import List, Iterable
import xarray
import pandas
import numpy
from coastalmodeling_vdatum import vdatum
from seanode.data_stores import DataStore
from seanode.analysis_task import AnalysisTask
import seanode.utils
import logging


logger = logging.getLogger(__name__)


class GridAnalysisTask(AnalysisTask):
    """A task for returning data from a single grid geometry file.

    Extends AnalysisTask for grid-based data, overriding the open_dataset
    and get_subset methods, but inheriting postprocess and run.

    Attributes
    ----------
    filename
    coords
    varlist
    timeslice
    stations
    file_format

    Methods
    -------
    open_dataset(data_store)
    get_subset(dataset)
    postprocess(output_datum)
    run()
    
    """

    def __init__(
        self,
        filename: str | list,
        coords: dict,
        varlist: List[dict],
        timeslice: tuple | None,
        stations: pandas.DataFrame,
        file_format: str
    ) -> None:
        """Create a GridAnalysisTask.

        Attributes
        ----------
        filename
            The full path of the file to process.
        coords
            A dictionary containing coordinate variables, having
            the form {"new/standard name" : "name in file"}.
        varlist
            A list of variable dictionaries formatted as in FieldSource objects.
        timeslice
            Either a tuple of length 2 (start, end) defining the bounds of time
            steps to be returned, or None, in which case no time subsetting is
            performed.
        stations
            An object describing the stations for which to return data, including
            locations and, optionally, names.
        file_format
            The format of this task's file. Usually one of 
            {"nc", "grib", "grib2"}.
            
        """
        super().__init__(filename, coords, varlist, timeslice, stations)
        self.file_format = file_format

    def open_dataset(self, store: DataStore) -> xarray.Dataset:
        """Open this task's dataset from given data store."""
        logger.info(f'opening file {self.filename}')
        return store.open_file(self.filename, format=self.file_format)

    def get_subset(self, ds:xarray.Dataset) -> pandas.DataFrame:
        """Subset this task's dataset and return a data frame.

        Subsetting proceeds by (1) variable, (2) time, (3) space.

        Parameters
        ----------
        ds
            Assumed to have coordinate and data variables listed in the
            class instance's coords and varlist. 

        Returns
        -------
        pandas DataFrame 
            Table with (station, time) multi-index containing data for all
            station locations. Coordinate variable names have been changed/
            standardized, but the data variable names have not.
            
        """
        # Rename coordinates.
        ds_sub = ds.rename({v:k for k,v in self.coords.items()})
        
        # Make sure longitude is in range [-180.0, 180.0]
        ds_sub['longitude'] = seanode.utils.switch_lon_lims(
            ds_sub['longitude'], min_lon=-180.0
        )
        ds_sub = ds_sub.sortby('longitude')
        
        # Get list of variables and coordinates to keep.
        file_var_list = [var_dict['varname_file'] for var_dict in self.varlist] + \
            list(self.coords.keys())
        # Subset variables.
        ds_sub = ds_sub[file_var_list]
        
        # Subset times (if applicable).
        if self.timeslice is not None:
            ds_sub = ds_sub.sel(
                time=slice(numpy.datetime64(self.timeslice[0]), 
                           numpy.datetime64(self.timeslice[1]))
            )
            
        # Subset stations (assumes stations has latitude and longitude
        # coordinates with a "station"-like dimension).
        if self.stations.index.name is None:
            if 'station' in self.stations:
                logger.debug('Setting variable station as station dimension in GridAnalysisTask station subsetter.')
                ds_stations = self.stations.set_index('station').to_xarray()
            elif 'station_name' in self.stations:
                logger.debug('Setting variable station_name as station dimension in GridAnalysisTask station subsetter.')
                ds_stations = self.stations\
                              .rename(columns={'station_name':'station'})\
                              .set_index('station')\
                              .to_xarray()
            else:
                logger.debug('Creating lat_lon coordinate to use as station dimension in GridAnalysisTask station subsetter.')
                df_stations = self.stations
                lat_lon = [f'{la:.5f}N {lo:.5f}E' for (la, lo) in 
                           zip(self.stations.latitude, self.stations.longitude)]
                df_stations.insert(0,'station', lat_lon)
                ds_stations = df_stations.set_index('station').to_xarray()
        df = ds_sub.sel(
            latitude=ds_stations.latitude,
            longitude=ds_stations.longitude,
            method='nearest'
        ).to_dataframe().reset_index().set_index(['station','time'])
        
        return df

    

    