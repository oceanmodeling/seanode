"""Define a task of opening, subsetting, and transforming a mesh file dataset.

A mesh structured file has data variables with dimensions that are distinct
from latitude and longitude. Instead longitude and latitude information are
contained in separate data varaibles. 

This could correspond to an unstructured mesh (one spatial dimension variable)
or an alternative grid (two spatial dimension variables, not equal to latitude
and longitude).


Classes
-------
MeshAnalysisTask
    The subclass of AnalysisTask class, with methods for mesh data files.
    
"""


from abc import ABC, abstractmethod
from typing import List, Iterable
import xarray
import pandas
import numpy
import scipy
from coastalmodeling_vdatum import vdatum
from seanode.data_stores import DataStore
from seanode.analysis_task import AnalysisTask
import seanode.utils
import logging
import warnings


logger = logging.getLogger(__name__)


class MeshAnalysisTask(AnalysisTask):
    """A task for returning data from a single mesh geometry file.

    Extends AnalysisTask for mesh-based data, overriding the open_dataset
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
        filename: str,
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
        ds = store.open_file(self.filename, format=self.file_format)
        logger.debug('file opened')
        logger.debug(ds)
        return ds

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
        logger.info('subsetting mesh dataset')
        # Rename coordinates and convert lat/lon to regular variables.
        logger.debug('renaming variables')
        ds_sub = ds.rename({v:k for k,v in self.coords.items()})
        try:
            ds_sub = ds_sub.reset_coords(['latitude', 'longitude'])
        except:
            logger.warning('Cannot convert latitude and longitude to data variables.')
        
        # Make sure longitude is in range [-180.0, 180.0]
        logger.debug('switching longitude')
        ds_sub['longitude'] = seanode.utils.switch_lon_lims(
            ds_sub['longitude'], min_lon=-180.0
        )
        
        # Get list of variables and coordinates to keep.
        logger.debug('subsetting variables')
        file_var_list = [var_dict['varname_file'] for var_dict in self.varlist] + \
            list(self.coords.keys())
        # Subset variables.
        ds_sub = ds_sub[file_var_list]
        
        # Subset times (if applicable).
        logger.debug('subsetting times')
        if self.timeslice is not None:
            ds_sub = ds_sub.sel(
                time=slice(numpy.datetime64(self.timeslice[0]), 
                           numpy.datetime64(self.timeslice[1]))
            )
        # If there are no timesteps, return empty dataframe.
        if ds_sub['time'].size == 0:
            logger.warning('No time steps found in dataset after time subsetting. Returning empty dataframe.')
            return pandas.DataFrame()
        
        # Get nearest model points.
        logger.debug('finding nearest mesh points')
        dists_inds = get_nearest_dists_inds(ds_sub, self.stations,
                                            n_nearest=3)

        # Try to add station coordinate if possible.
        if 'station' in self.stations:
            logger.debug('Setting variable station as station dimension in MeshAnalysisTask station subsetter.')
            dists_inds.coords['station'] = self.stations['station']
        elif 'station_name' in self.stations:
            logger.debug('Setting variable station_name as station dimension in MeshAnalysisTask station subsetter.')
            dists_inds.coords['station'] = self.stations['station_name']
        else:
            logger.debug('Creating lat_lon coordinate to use as station dimension in MeshAnalysisTask station subsetter.')
            lat_lon = [f'{la:.5f}N_{lo:.5f}E' for (la, lo) in 
                       zip(self.stations.latitude, self.stations.longitude)]
            dists_inds.coords['station'] = lat_lon
        
        # Subset stations (assumes stations has latitude and longitude
        # coordinates with a "station"-like dimension).
        # We do this with a try statement cause the dataset-based indexing
        # sometimes doesn't work for a lazy-loaded dataset.
        # This avoids an exception like:
        #     AttributeError: 'ScipyArrayWrapper' object has no attribute 'vindex'
        logger.debug('subsetting to nearest points')
        try:
            ds_sub_test = ds_sub.isel({d:dists_inds[d] for d in ds_sub['longitude'].dims})
            test = ds_sub_test.to_dataframe()
        except:
            logger.debug('Manually loading dataset before station subsetting.')
            ds_sub.load()
            ds_sub_test = ds_sub.isel({d:dists_inds[d] for d in ds_sub['longitude'].dims})
        ds_sub = ds_sub_test
        
        # Calculate inverse distance weights.
        logger.debug('calculating weighted average of nearby points')
        weights = calc_inv_dist_wts(dists_inds['distance'], exponent=1)
        # TODO: Add a version of weight calculation that takes the nearest
        # N (e.g., 3) non-missing points and calculates weights based on those.
        
        # Apply the weights.
        ds_sub = ds_sub * weights
        
        # Sum along the "k" dimension, leaving "time" and "station".
        ds_sub = ds_sub.sum(dim='k')

        # Convert dataset to data frame.
        logger.debug('converting to data frame')
        df = ds_sub.to_dataframe().reset_index().set_index(['station','time'])
        
        return df


def get_nearest_dists_inds(ds_mesh, df_stations,
                           n_nearest=10,
                           ds_x_var='longitude', ds_y_var='latitude',
                           df_x_var='longitude', df_y_var='latitude'):
    """Returns distances to and indexes of nearest mesh/grid points.

    Should work for any dimension (usually 1 [mesh] or 2 [grid]) of
    latitude and longitude arrays.

    Parameters
    ----------
    ds_mesh
        Dataset containing latitude, longitude, and other data
        variables. 
    df_stations
        Data frame containing information about the locations to
        extract data at.
    n_nearest
        The number of nearest points to extract data for around
        each location in df_stations
    ds_x_var
        The name of the longitude variable in ds_mesh
    ds_y_var
        The name of the latitude variable in ds_mesh
    df_x_var
        The name of the longitude variable in df_stations
    df_y_var
        The name of the latitude variable in df_stations

    Returns
    -------
    Dataset of distances and indices.
        Contains a distance array, and one index array for 
        each of the spatial dimensions of the latitude & longitude 
        variables. Each of these data arrays has shape 
        (len(df_stations), N_nearest).
        
    """
    tree = scipy.spatial.cKDTree(
        numpy.column_stack(
            (numpy.ravel(ds_mesh[ds_x_var].data),
             numpy.ravel(ds_mesh[ds_y_var].data))
        )
    )
    dists, inds = tree.query(
        numpy.column_stack(
            (df_stations[df_x_var], 
             df_stations[df_y_var])
        ), 
        k=n_nearest
    )
    
    # The outputs above are either 1-dimensional (station), if k=1,
    # or 2-d (station, k), if k>1. We want them to be 2-d (station, k).
    if n_nearest == 1:
        dists  = numpy.expand_dims(dists, axis=1)
        inds  = numpy.expand_dims(inds, axis=1)
   
    # Next, we need to adjust the indices to reflect the 
    # non-raveled spatial dimensions. 
    # This creates a tuple of arrays, where each array has shape
    # (N_stations, k), and the tuple has as many elements as 
    # ds_mesh has horizontal spatial dimensions
    # [usually 1 (for unstructured mesh) or 2 (for gridded data)].
    inds_expd = numpy.unravel_index(inds, ds_mesh[ds_x_var].shape)
   
    # Now we construct a dataset that describes the different arrays.
    data_vars = {'distance':(['station', 'k'], dists)}
    for i_dim, dim in enumerate(ds_mesh[ds_x_var].dims):
        data_vars[dim] = (['station', 'k'], inds_expd[i_dim])
    result = xarray.Dataset(data_vars=data_vars)
    
    return result


def calc_inv_dist_wts(distances, exponent=1):
    """Calculate inverse distance weights across a set of locations.

    In the case where one of the distances is exactly 0, the result
    is set to exactly that value. 

    Parameters
    ----------
    distances
        DataArray of distances with shape (N_stations, N_nearest).
        The weights are calculated along the N_nearest axis, such
        that the result sums to 1.0 along that axis.
    exponent
        The exponent/power to which inverse distances are raised.

    Returns
    -------
    Array of weights
        Has the same shape as dists.
        
    """
    if type(distances) == xarray.DataArray:
        dists = distances.data
        outdims = distances.dims
    elif type(distances) == numpy.ndarray:
        dists = distances
        outdims = ['station', 'k']
    else:
        warnings.warn('calc_inv_dist_wts() expects a numpy array or xarray DataArray as input.')
    wts = numpy.where(
        dists == 0.0, 
        1.0, 
        ~numpy.any(dists == 0.0, axis=1, keepdims=True) / (dists ** exponent)
    )
    wts = wts / numpy.sum(wts, axis=1, keepdims=True)
    result = xarray.DataArray(wts, dims=outdims)
    return result


    