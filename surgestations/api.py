"""Contains function to create and run a STOFS station data request. 
"""


from surgestations import request_options
from surgestations.request import SurgeModelRequest
import pandas


def get_surge_model_at_stations(
    model,
    variables,
    stations,
    start_date,
    end_date,
    forecast_type,
    file_geometry,
    output_datum,
    data_store='AWS'
) -> pandas.DataFrame:
    """
    """
    # Parse the options.
    if model in ['STOFS_2D_GLO']:
        req_model = request_options.ModelOptions.STOFS_2D_GLO
    elif model in ['STOFS_3D_ATL']:
        req_model = request_options.ModelOptions.STOFS_3D_ATL
    else:
        model_opts = [m.name for m in list(request_options.ModelOptions)]
        sys.exit(f'model {model} not recognized. Try one of {model_opts}.')
        
    if forecast_type in ['forecast']:
        req_forecast_type = request_options.ForecastType.FORECAST
        req_start_date = start_date
        # Ignore end date for forecast option.
        req_end_date = start_date
    elif forecast_type in ['nowcast']:
        req_forecast_type = request_options.ForecastType.NOWCAST
        req_start_date = start_date
        req_end_date = end_date
    else:
        ft_opts = [ft.name for ft in list(request_options.ForecastType)]
        sys.exit(f'forecast type {forecast_type} not recognized. Try one of {ft_opts}.')

    if file_geometry in ['points']:
        req_file_geometry = request_options.FileGeometry.POINTS
    elif file_geometry in ['mesh']:
        req_file_geometry = request_options.FileGeometry.MESH
    elif file_geometry in ['grid']:
        req_file_geometry = request_options.FileGeometry.GRID
    else:
        fg_opts = [fg.name for fg in list(request_options.FileGeometry)]
        sys.exit(f'file geometry {file_geometry} not recognized. Try one of {fg_opts}.')

    if data_store in ['AWS']:
        req_data_store = request_options.DataStoreOptions.AWS
    else:
        ds_opts = [ds.name for ds in list(request_options.DataStoreOptions)]
        sys.exit(f'data store {data_store} not recognized. Try one of {ds_opts}.')

    if req_end_date < req_start_date:
        sys.exit('end_date must be later than or equal to start_date.')
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