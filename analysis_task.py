"""
"""


# External libraries
from abc import ABC, abstractmethod
from typing import List, Iterable
import xarray
import pandas
import numpy
# This package
from data_stores import DataStore


class AnalysisTask:
    """
    """

    def __init__(
        self,
        filename: str,
        vars: List[dict],
        timeslice: tuple | None,
        stations: pandas.DataFrame
    ) -> None:
        """
        """
        self.filename = filename
        self.vars = vars
        self.timeslice = timeslice
        self.stations = stations

    def open_dataset(self, store: DataStore) -> xarray.Dataset:
        """
        """
        print(f'opening file {self.filename}')
        return store.open_file(self.filename)

    def get_subset(self, ds:xarray.Dataset) -> pandas.DataFrame:
        """
        """
        # TODO: find better way to add ancillary variables to the subset.
        file_var_list = [var_dict['varname_file'] for var_dict in self.vars]
        file_var_list.append('x')
        file_var_list.append('y')
        file_var_list.append('station_name')
        
        ds_sub = ds[file_var_list].sel(
            time=slice(numpy.datetime64(self.timeslice[0]), 
                       numpy.datetime64(self.timeslice[1]))
        )
        # This might change if we subclass to 
        # PointsAnalysisTask and MeshAnalysisTask.
        df = extract_stations_by_nos_id(ds_sub, self.stations)
        return df

    def postprocess(self):
        """
        """
        pass

    def run(self, store: DataStore) -> pandas.DataFrame:
        """
        """
        with self.open_dataset(store).load() as ds:
            self.dataframe = self.get_subset(ds)
        # self.postprocess()
        return self.dataframe


def extract_stations_by_nos_id(
        ds: xarray.Dataset, 
        station_id_list: Iterable
    ) -> pandas.DataFrame:
    """Extract dataframe of model data at listed station locations.

    Starting from an xarray dataset that contains model data at station 
    locations, this function extracts data from variable data_var at
    station locations listed in station_id_list, and returns a pandas
    data frame in the same format as used for observational data. 

    Parameters
    ----------
    ds 
        Contains variable data_var, with dimensions [time, station], 
        and variable "station_name", also with dimensions [time, station],
        that is searched for matches with stations listed in station_id_list.
    station_id_list
        An iterable (e.g., list, pandas Index, or pandas Series) containing
        station IDs that are matched with "station_name" in ds.

    Returns
    -------
    pandas DataFrame
        Table with (station, time) multi-index containing data for all
        station locations for variable data_var.

    """
    result = pandas.DataFrame()

    for nos_id in station_id_list:

        # Find the model station names that match this NOS ID.
        obs_name_in_model = [nos_id in nm.decode('utf-8') 
                             for nm in ds.station_name.data]

        # Check that only one model station matchis this ID.
        if numpy.sum(obs_name_in_model) > 1:
            print(f"Warning: more than one model station matches NOS ID {nos_id}")
        elif numpy.sum(obs_name_in_model) == 0:
            print(f"Warning: no model station matches for NOS ID {nos_id}")
        else:   
            # If available, concatenate the data with other stations.
            station_df = ds.loc[dict(station=obs_name_in_model)]\
                .assign_coords(station=numpy.array([nos_id]))\
                .to_dataframe(dim_order=('station', 'time'))
            try:
                result = pandas.concat(
                    [result, station_df],
                    axis=0,
                    join="outer",
                     ignore_index=False,
                     sort=False
                )
            except:
                print(f"Warning: cannot concatenate station data frame for NOS ID {nos_id}: Skipping.")
                continue

    return result
            