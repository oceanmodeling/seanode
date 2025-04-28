"""Define a task of opening, subsetting, and transforming a single file dataset.

This is currently geared towards Point type data files, but serves as the 
base class for mesh and grid file analysis tasks.

Classes
-------
AnalysisTask
    The base class with methods for point data files.
STOFS3DAtlAnalysisTask
    A subclass with a special open_dataset method that
    deals with incorrect latitude and longitude variables
    in STOFS-3D-Atlantic Points files.

Methods
-------
extract_stations_by_nos_id

"""


from abc import ABC, abstractmethod
from typing import List, Iterable
import xarray
import pandas
import numpy
from coastalmodeling_vdatum import vdatum
from seanode.data_stores import DataStore
import logging


logger = logging.getLogger(__name__)


class AnalysisTask:
    """A task for returning data from a single file.

    This functions as the base class for other analysis task classes,
    but also works as a concrete class in its own right. The task will 
    usually be initialized and run(), but the other methods are public
    in case a more granular approach is needed.

    Attributes
    ----------
    filename
    coords
    varlist
    timeslice
    stations

    Methods
    -------
    open_dataset(data_store)
    get_subset(dataset)
    postprocess(output_datum)
    run()
    
    """

    def __init__(
        self,
        filename: str,
        coords: dict,
        varlist: List[dict],
        timeslice: tuple | None,
        stations: Iterable | pandas.DataFrame
    ) -> None:
        """Create an AnalysisTask.

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
            names and/or locations.
            
        """
        self.filename = filename
        self.coords = coords
        self.varlist = varlist
        self.timeslice = timeslice
        self.stations = stations

    def open_dataset(self, store: DataStore) -> xarray.Dataset:
        """Open this task's dataset from given data store."""
        logger.info(f'opening file {self.filename}')
        return store.open_file(self.filename)

    def get_subset(self, ds:xarray.Dataset) -> pandas.DataFrame:
        """Subset a dataset and return a data frame.

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
        """Postprocess this task's data frame.

        To be performed after subsetting. This method:
        1. Renames data variables.
        2. Converts datum of water level variables.
        
        Parameters
        ----------
        output_datum
            The datum to convert water level variables to. Usually an
            abbreviation like 'mllw'.

        Returns
        -------
        None
        
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
        """Open, subset, and postprocess data for this task.

        A convenience wrapper to sequentially perform 
        1. open_dataset,
        2. get_subset, then 
        3. postprocess, 
        and return the final state of the task's data frame.

        Parameters
        ----------
        store
            The data store, passed to open_dataset.
        output_datum
            The datum to convert water level variables to. Passed 
            directly to the postprocess method.
            
        Returns
        -------
        pandas DataFrame
            Table containing the subsetted and processed data for this
            AnalysisTask.
            
        """
        with self.open_dataset(store) as ds:
            self.dataframe = self.get_subset(ds)
        self.postprocess(output_datum)
        return self.dataframe


class STOFS3DAtlAnalysisTask(AnalysisTask):
    """A task for returning data from a single STOFS 3D Points file.

    The difference with the base class is a special open_dataset method
    that optionally fixes erroneous latitude and longitude variables in 
    some STOFS-3D-Atlantic Points files. Additionally, there is an 
    attribute to control the optional fix.

    Attributes
    ----------
    filename
    coords
    varlist
    timeslice
    stations
    switch_xy

    Methods
    -------
    open_dataset(data_store)
    
    """

    def __init__(
        self,
        filename: str,
        coords: dict,
        varlist: List[dict],
        timeslice: tuple | None,
        stations: Iterable | pandas.DataFrame,
        switch_xy: bool = False
    ) -> None:
        """Initialize a STOFS3DAtlAnalysisTask object.

        Parameters
        ----------
        All as in the base class method, plus:
        switch_xy
            Flag controlling whether to switch the x and y variables, thereby
            fixing the error in the coordinates. This is set by the routine 
            that create the analysis task, based on the date requested.
        
        """
        super().__init__(filename, coords, varlist, timeslice, stations)
        self.switch_xy = switch_xy
    
    def open_dataset(self, store: DataStore) -> xarray.Dataset:
        """Open this task's dataset from given store.

        This overrides the parent class method in order to switch the
        x and y variables, which have been misnamed (with a few exceptions).

        Parameters
        ----------
        store
            The DataStore object representing a data storage system. 

        Returns
        -------
        xarray Dataset
            The data from this task's file, with the x and y 
            coordinate variables fixed if appropriate.
        
        """
        logger.info(f'opening file {self.filename}')
        ds = store.open_file(self.filename)

        if self.switch_xy:
            logger.warning('Switching x and y in STOFS3DAtlAnalysisTask')
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
        Contains data variables, with dimensions [time, station], 
        and variable "station_name", with dimensions [station],
        that is searched for matches with stations listed in station_id_list.
    station_id_list
        An iterable (e.g., list, pandas Index, or pandas Series) containing
        station IDs that are matched with "station_name" in ds.

    Returns
    -------
    pandas DataFrame
        Table with (station, time) multi-index containing data for all
        station locations.

    """
    result = pandas.DataFrame()

    for nos_id in station_id_list:

        # Find the model station names that match this NOS ID.
        obs_name_in_model = [nos_id in nm.decode('utf-8') 
                             for nm in ds.station_name.data]

        # Check that only one model station matchis this ID.
        if numpy.sum(obs_name_in_model) > 1:
            logger.warning(f"More than one model station matches NOS ID {nos_id}")
        elif numpy.sum(obs_name_in_model) == 0:
            logger.warning(f"No model station matches for NOS ID {nos_id}")
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
                logger.warning(f"Cannot concatenate station data frame for NOS ID {nos_id}: Skipping.")
                continue

    return result
            