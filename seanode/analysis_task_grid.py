"""
"""


# External libraries
from abc import ABC, abstractmethod
from typing import List, Iterable
import xarray
import pandas
import numpy
from coastalmodeling_vdatum import vdatum
# This package
from seanode.data_stores import DataStore
from seanode.analysis_task import AnalysisTask
import seanode.utils


class GridAnalysisTask(AnalysisTask):
    """
    """

    def __init__(
        self,
        filename: str,
        coords: dict,
        varlist: List[dict],
        timeslice: tuple | None,
        stations: pandas.DataFrame,
        file_format: str
    ) -> None:
        super().__init__(filename, coords, varlist, timeslice, stations)
        self.file_format = file_format

    def open_dataset(self, store: DataStore) -> xarray.Dataset:
        """
        """
        print(f'opening file {self.filename}')
        return store.open_file(self.filename, format=self.file_format)

    def get_subset(self, ds:xarray.Dataset) -> pandas.DataFrame:
        """
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
                ds_stations = self.stations.set_index('station').to_xarray()
            elif 'station_name' in self.stations:
                ds_stations = self.stations\
                              .rename(columns={'station_name':'station'})\
                              .set_index('station')\
                              .to_xarray()
            else:
                df_stations = self.stations
                lat_lon = [f'{la:.5f}N {lo:.5f}E' for (la, lo) in 
                           zip(self.stations.latitude, self.stations.longitude)]
                df_stations.insert(0,'station', lat_lon)
                ds_stations = df_stations.set_index('lat_lon').to_xarray()
        df = ds_sub.sel(
            latitude=ds_stations.latitude,
            longitude=ds_stations.longitude,
            method='nearest'
        ).to_dataframe().reset_index().set_index(['station','time'])
        return df

    

    