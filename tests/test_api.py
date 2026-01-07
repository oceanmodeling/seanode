import pathlib  
import sys
sys.path.append(str(pathlib.Path(__file__).parent.parent.absolute()))
import datetime
import pandas as pd
from seanode.api import get_surge_model_at_stations
import logging


logging.basicConfig(level=logging.INFO)


# Define parameters that should create a working request.
model = 'STOFS_2D_GLO'
var_list = ['cwl_bias_corrected', 'u_vel', 'v_vel']
stations = pd.Series(['8720218', '8720357', '8725114'])
start_date = datetime.datetime(2024,12,1,12,0)
end_date = datetime.datetime(2024,12,2,12,0)
forecast_type = 'nowcast'
geometry = 'points'
datum = 'MLLW'
store = 'AWS'

# Define parameters that should not create working requests.
bad_model = 'BAD_MODEL'
bad_var_list = ['cwl_bias_corrected', 'BAD_VAR']
empty_stations_df = pd.DataFrame()
empty_stations_series = pd.Series([])
stations_no_latlon = pd.DataFrame({'station':['8720218','8720357','8725114']})
stations_no_station = pd.DataFrame({'latitude':[35.0, 40.0,45.0]})
future_start_date = datetime.datetime(2100,1,1,0,0)
end_date_before_start = datetime.datetime(2000,1,1,0,0)
bad_forecast_type = 'BAD_FORECAST_TYPE'
bad_geometry = 'BAD_GEOMETRY'
bad_datum = 'BAD_DATUM'
bad_store = 'BAD_STORE'

# Define the test functions.
def test_bad_model():
    try:
        df = get_surge_model_at_stations(
            bad_model,
            var_list,
            stations,
            start_date,
            end_date,
            forecast_type,
            geometry,
            datum,
            store
        )
    except ValueError as e:
        assert f'model {bad_model} not recognized' in str(e), "Error message should mention 'model BAD_MODEL not recognized'."
    else:
        assert False, "Expected ValueError for bad model."

def test_empty_stations_df():
    try:
        df = get_surge_model_at_stations(
            model,
            var_list,
            empty_stations_df,
            start_date,
            end_date,
            forecast_type,
            geometry,
            datum,
            store
        )
    except ValueError as e:
        assert 'stations data frame is empty' in str(e), "Error message should mention 'stations data frame is empty'."
    else:
        assert False, "Expected ValueError for empty stations DataFrame."

def test_empty_stations_series():
    try:
        df = get_surge_model_at_stations(
            model,
            var_list,
            empty_stations_series,
            start_date,
            end_date,
            forecast_type,
            geometry,
            datum,
            store
        )
    except ValueError as e:
        assert 'empty' in str(e), "Error message should mention 'empty'."
    else:
        assert False, "Expected ValueError for empty stations Series."

def test_bad_forecast_type():
    try:
        df = get_surge_model_at_stations(
            model,
            var_list,
            stations,
            start_date,
            end_date,
            bad_forecast_type,
            geometry,
            datum,
            store
        )
    except ValueError as e:
        assert f'forecast type {bad_forecast_type} not recognized' in str(e), "Error message should mention 'forecast type BAD_FORECAST_TYPE not recognized'."
    else:
        assert False, "Expected ValueError for bad forecast type."

def test_bad_geometry():
    try:
        df = get_surge_model_at_stations(
            model,
            var_list,
            stations,
            start_date,
            end_date,
            forecast_type,
            bad_geometry,
            datum,
            store
        )
    except ValueError as e:
        assert f'file geometry {bad_geometry} not recognized' in str(e), "Error message should mention 'file geometry BAD_GEOMETRY not recognized'."
    else:
        assert False, "Expected ValueError for bad geometry."

def test_points_missing_stations():
    try:
        df = get_surge_model_at_stations(
            model,
            var_list,
            stations_no_station,
            start_date,
            end_date,
            forecast_type,
            'points',
            datum,
            store
        )
    except ValueError as e:
        assert 'stations data frame must have a "station" column for points file geometry' in str(e), "Error message should mention 'stations data frame must have a station column for points file geometry.'."
    else:
        assert False, "Expected ValueError for missing station for points geometry."

def test_mesh_missing_latlon():
    try:
        df = get_surge_model_at_stations(
            model,
            var_list,
            stations_no_latlon,
            start_date,
            end_date,
            forecast_type,
            'mesh',
            datum,
            store
        )
    except ValueError as e:
        assert 'stations data frame must have "latitude" and "longitude" columns for mesh file geometry' in str(e), "Error message should mention 'stations data frame must have latitude and longitude columns for mesh file geometry.'."
    else:
        assert False, "Expected ValueError for missing lat/lon in mesh geometry."

def test_grid_missing_latlon():
    try:
        df = get_surge_model_at_stations(
            model,
            var_list,
            stations_no_latlon,
            start_date,
            end_date,
            forecast_type,
            'grid',
            datum,
            store
        )
    except ValueError as e:
        assert 'stations data frame must have "latitude" and "longitude" columns for grid file geometry' in str(e), "Error message should mention 'stations data frame must have latitude and longitude columns for grid file geometry.'."
    else:
        assert False, "Expected ValueError for missing lat/lon in grid geometry."

def test_bad_data_store():
    try:
        df = get_surge_model_at_stations(
            model,
            var_list,
            stations,
            start_date,
            end_date,
            forecast_type,
            geometry,
            datum,
            bad_store
        )
    except ValueError as e:
        assert f'data store {bad_store} not recognized' in str(e), "Error message should mention 'data store BAD_STORE not recognized'."
    else:
        assert False, "Expected ValueError for bad data store."

def test_bad_datum():
    try:
        df = get_surge_model_at_stations(
            model,
            var_list,
            stations,
            start_date,
            end_date,
            forecast_type,
            geometry,
            bad_datum,
            store
        )
    except ValueError as e:
        assert f'output datum {bad_datum} not recognized' in str(e), "Error message should mention 'output datum BAD_DATUM not recognized'."
    else:
        assert False, "Expected ValueError for bad datum."

def test_end_before_start():
    try:
        df = get_surge_model_at_stations(
            model,
            var_list,
            stations,
            start_date,
            end_date_before_start,
            forecast_type,
            geometry,
            datum,
            store
        )
    except ValueError as e:
        assert 'end_date must be later than or equal to start_date' in str(e), "Error message should mention 'end_date must be later than or equal to start_date.'."
    else:
        assert False, "Expected ValueError for end date before start date."

def test_future_start_date():
    try:
        df = get_surge_model_at_stations(
            model,
            var_list,
            stations,
            future_start_date,
            future_start_date,
            forecast_type,
            geometry,
            datum,
            store
        )
    except ValueError as e:
        assert 'start_date cannot be in the future' in str(e), "Error message should mention 'start_date cannot be in the future.'."
    else:
        assert False, "Expected ValueError for future start date."

if __name__ == "__main__":
    # Run all the tests.
    test_bad_model()
    test_empty_stations_df()
    test_empty_stations_series()
    test_bad_forecast_type()
    test_bad_geometry()
    test_points_missing_stations()
    test_mesh_missing_latlon()
    test_grid_missing_latlon()
    test_bad_data_store()
    test_bad_datum()
    test_end_before_start()
    test_future_start_date()
    print("All tests passed.")
