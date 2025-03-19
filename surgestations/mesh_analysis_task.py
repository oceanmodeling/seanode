"""
This might eventually change to be subclassed as 
PointsAnalysisTask and MeshAnalysisTask.
"""


# External libraries
from abc import ABC, abstractmethod
from typing import List, Iterable
import xarray
import pandas
import scipy
import numpy
from coastalmodeling_vdatum import vdatum
# This package
from surgestations.data_stores import DataStore
from surgestations.analysis_task import AnalysisTask


class MeshAnalysisTask(AnalysisTask):
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

    def get_subset(self, ds:xarray.Dataset) -> pandas.DataFrame:
        """
        """
        file_var_list = [var_dict['varname_file'] for var_dict in self.vars]
        
        ds_sub = ds[file_var_list]
        if self.timeslice is not None:
            ds_sub = ds_sub.sel(
                time=slice(numpy.datetime64(self.timeslice[0]), 
                           numpy.datetime64(self.timeslice[1]))
            )

        # Construct tree to find nearest nodes.
        tree = scipy.spatial.cKDTree(
            numpy.column_stack((ds.x.data,
                             ds.y.data))
        )
        
        # Get 3 nearest neighbors for interpolation.
        dists, inds = tree.query(
            tuple(zip(self.stations['longitude'], self.stations['latitude'])), 
            k=3
        )
        # dists and inds both have shape (N_stations, k=3).
        
        # Calculate inverse distance weights.
        weights = calc_inv_dist_wts(dists, exponent=1)
        
        # Get the required nodes.
        ds_sub = ds_sub.isel(node=xarray.DataArray(inds, dims=["station", "k"]))
        # This replaces the "node" dimension with two new dimensions: "station" and "k".

        # Calculate the weighted averages.
        # This seems to be the slow step.
        ds_sub = ds_sub*xarray.DataArray(weights, dims=["station", "k"])
        #
        ds_sub = ds_sub.sum(dim='k')
        # This collapses the "k" dimension, leaving "time" and "station".
        
        return ds_sub.to_dataframe(dim_order=['station', 'time']).join(self.stations, on='station', how='left')


def calc_inv_dist_wts(dists, exponent=1):
    """
    """
    wts = numpy.where(
        dists == 0.0, 
        1.0, 
        ~numpy.any(dists == 0.0, axis=1, keepdims=True) / (dists**exponent)
    )
    return wts / numpy.sum(wts, axis=1, keepdims=True)


            