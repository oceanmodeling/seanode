"""
This might eventually change to be subclassed as 
PointsAnalysisTask and MeshAnalysisTask.
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


class AnalysisTask:
    """
    """

    def __init__(
        self,
        filename: str,
        coords: dict,
        varlist: List[dict],
        timeslice: tuple | None,
        stations: pandas.DataFrame
    ) -> None:
        """
        """
        self.filename = filename
        self.coords = coords
        self.varlist = varlist
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
        # Rename coordinates.
        ds_sub = ds.rename({v:k for k,v in self.coords.items()})
        # Get list of variables and coordinates to keep.
        file_var_list = [var_dict['varname_file'] for var_dict in self.varlist] + \
            list(self.coords.keys())
        # Subset variables.
        ds_sub = ds_sub[file_var_list]
        # Subset time.
        if self.timeslice is not None:
            ds_sub = ds_sub.sel(
                time=slice(numpy.datetime64(self.timeslice[0]), 
                           numpy.datetime64(self.timeslice[1]))
            )
        # Subset stations. 
        # TODO: This might change if we subclass to 
        # PointsAnalysisTask and MeshAnalysisTask.
        df = extract_stations_by_nos_id(ds_sub.load(), self.stations)
        return df

    def postprocess(self, output_datum: str) -> None:
        """
        """
        # Rename columns.
        col_name_mapper = {vdict['varname_file']:vdict['varname_out'] 
                           for vdict in self.varlist}
        self.dataframe = self.dataframe.rename(columns=col_name_mapper)
        
        # Datum conversion.
        if output_datum is not None:
            for vdict in self.varlist:
                if vdict['datum'] is not None:
                    _, _, z_conv = vdatum.convert(
                        vdict['datum'].lower(), output_datum.lower(), 
                        self.dataframe['latitude'].values, 
                        self.dataframe['longitude'].values, 
                        self.dataframe[vdict['varname_out']].values,
                        online=True, epoch=None
                    )
                    self.dataframe[vdict['varname_out']] = z_conv
        
        return

    def run(self, store: DataStore, output_datum: str) -> pandas.DataFrame:
        """
        """
        with self.open_dataset(store) as ds:
            self.dataframe = self.get_subset(ds)
        self.postprocess(output_datum)
        return self.dataframe


class STOFS3DAtlAnalysisTask(AnalysisTask):
    """
    """

    def __init__(
        self,
        filename: str,
        coords: dict,
        varlist: List[dict],
        timeslice: tuple | None,
        stations: pandas.DataFrame,
        switch_xy: bool = False
    ) -> None:
        super().__init__(filename, coords, varlist, timeslice, stations)
        self.switch_xy = switch_xy
    
    def open_dataset(self, store: DataStore) -> xarray.Dataset:
        """Open this task's dataset from given store.

        This overrides the parent class method in order to switch the
        x and y variables, which have been misnamed (with a few exceptions).
        
        """
        print(f'opening file {self.filename}')
        ds = store.open_file(self.filename)

        if self.switch_xy:
            print('Switching x and y in STOFS3DAtlAnalysisTask')
            ds = ds.rename({'x':'actual_latitude', 'y':'actual_longitude'})
            ds = ds.rename({'actual_latitude':'y', 'actual_longitude':'x'})
            # Need to switch a few back, cause they were the right way round
            # to begin with....
            for st in [
                'TRDF1 SOUS41 8721604 FL Trident Pier, Port Canaver',
                'LCLL1 SOUS42 8767816 LA Lake Charles',
                'LUIT2 SOUS42 8771972 TX San Luis Pass'
            ]:
                obs_name_in_model = [
                    st in nm.decode('utf-8') 
                    for nm in ds.station_name.data
                ] 
                temp_lat = ds.x.data[obs_name_in_model] 
                temp_lon = ds.y.data[obs_name_in_model]
                ds.x.data[obs_name_in_model] = temp_lon
                ds.y.data[obs_name_in_model] = temp_lat
        
        return ds
    


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
            