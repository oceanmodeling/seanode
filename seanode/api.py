"""Top level functions to request model data at point locations. 

These are the functions that should usually be called when using seanode 
in another project. Some checking of the parameters is done here, but it
is still possible that this code could allow options that end up not 
working.

See usage examples in example_points_query.ipynb and test_<model>.py files.

Functions
---------
get_surge_model_at_stations

"""


import sys
import pandas
import datetime
from typing import List, Iterable
from seanode import request_options
from seanode.request import SurgeModelRequest


def get_surge_model_at_stations(
    model: str,
    variables: List[str],
    stations: Iterable | pandas.DataFrame,
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    forecast_type: str,
    file_geometry: str,
    output_datum: str,
    data_store: str = 'AWS'
) -> pandas.DataFrame:
    """Get model data at discrete point locations.

    These point locations may correspond to stations, or could just be 
    points of interest.

    Parameters
    ----------
    model
        The name of the model to extract data from.
    variables
        The data variables to extract from the model.
    stations
        A list of locations to extract data at. If pulling from
        pre-defined station files (see file_geometry, below) this
        can just be an iterable of names or IDs.
        For grid or mesh file types, this must be a pandas data frame
        with "latitude" and "longitude" columns (and ideally a "station"
        column that can be used to index the locations).
    start_date
        The start date and time of the period of data to extract. For 
        nowcast data (see forecast_type, below), this is a simple start
        time of the extracted time series. For forecast data, start_date
        determines which forecast cycle is extracted (the latest that 
        contains start_date).
    end_date
        For nowcast data, the end of the extracted time series. For 
        forecast data, this parameter is ignored.
    forecast_type
        One of {"nowcast", "forecast"}. These terms have particular 
        meanings related to STOFS model cycles.
    file_geometry
        One of {"points", "grid", "mesh"}, used to determine what kind
        of model data files to extract from. If available, "points" is 
        usually much quicker than "grid" or "mesh".
    output_datum
        The datum relative to which water level data are returned. 
        Allowed options are determined by the coastalmodeling_vdatum
        package. Examples: "MLLW", "LMSL".
    data_store
        The location/file system where data are stored. Usually will
        be "AWS".

    Returns
    -------
    pandas.DataFrame
        A pandas data frame containing model data at station locations,
        organized with a (station, time) multi-index, and columns 
        containing data variables and other ancillary data (e.g., latitude
        and longitude).
        
    """
    # Parse the options.
    if model in ['STOFS_2D_GLO']:
        req_model = request_options.ModelOptions.STOFS_2D_GLO
    elif model in ['STOFS_3D_ATL']:
        req_model = request_options.ModelOptions.STOFS_3D_ATL
    elif model in ['GFS']:
        req_model = request_options.ModelOptions.GFS
    else:
        model_opts = [m.name for m in list(request_options.ModelOptions)]
        raise ValueError(f'model {model} not recognized. Try one of {model_opts}.')
        
    if forecast_type.lower() in ['forecast']:
        req_forecast_type = request_options.ForecastType.FORECAST
        req_start_date = start_date
        # Ignore end date for forecast option.
        req_end_date = start_date
    elif forecast_type.lower() in ['nowcast']:
        req_forecast_type = request_options.ForecastType.NOWCAST
        req_start_date = start_date
        req_end_date = end_date
    else:
        ft_opts = [ft.name for ft in list(request_options.ForecastType)]
        raise ValueError(f'forecast type {forecast_type} not recognized. Try one of {ft_opts}.')

    if file_geometry.lower() in ['points']:
        req_file_geometry = request_options.FileGeometry.POINTS
    elif file_geometry.lower() in ['mesh']:
        req_file_geometry = request_options.FileGeometry.MESH
    elif file_geometry.lower() in ['grid']:
        req_file_geometry = request_options.FileGeometry.GRID
    else:
        fg_opts = [fg.name for fg in list(request_options.FileGeometry)]
        raise ValueError(f'file geometry {file_geometry} not recognized. Try one of {fg_opts}.')

    if data_store.lower() in ['aws']:
        req_data_store = request_options.DataStoreOptions.AWS
    else:
        ds_opts = [ds.name for ds in list(request_options.DataStoreOptions)]
        raise ValueError(f'data store {data_store} not recognized. Try one of {ds_opts}.')

    if req_end_date < req_start_date:
        raise ValueError('end_date must be later than or equal to start_date.')
    # Note that dates are also defined/checked in the ForecastType section above.

    # TODO:
    # Add checks for variables?
    # Add check for station data frame formatting?
    # Add date conversions (numpy/pandas to datetime)?

    # Create the request.
    request = SurgeModelRequest(
        req_model,
        variables,
        stations,
        req_start_date,
        req_end_date,
        req_forecast_type,
        req_file_geometry,
        output_datum,
        req_data_store
    )
    
    # Run the request.
    result = request.run()
    return result