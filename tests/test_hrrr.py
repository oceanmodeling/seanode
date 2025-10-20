import pathlib  
import sys
sys.path.append(str(pathlib.Path(__file__).parent.parent.absolute()))
import datetime
import pandas as pd
import seanode
from seanode.models.hrrr import HRRRTaskCreator
import logging


logging.basicConfig(level=logging.INFO)


# Get a task creator instance.
hrrr = HRRRTaskCreator()

# Define some test specifications.
df_stations = pd.DataFrame(
    data={
        'station':['8720218', '8720357', '8447930'],
        'latitude':[30 + (23.9/60.0), 30 + (11.5/60.0), 41 + (31.4/60.0)],
        'longitude':[-81 - (25.7/60.0), -81 - (41.3/60.0), -70 - (40.3/60.0)]
    }
)
dt_start = datetime.datetime(2024,12,1,12,0)
dt_end = datetime.datetime(2024,12,2,12,0)
dt_future = datetime.datetime(2035,12,1,12,0)


# Define the tests.
# ----------------------------------------------------------------
# Nowcast.
def test_get_analysis_task_nowcast_current_version():
    atn = hrrr.get_analysis_tasks(
        ['ps', 'u10', 'v10'],
        df_stations,
        dt_start,
        dt_end,
        seanode.request_options.ForecastType.NOWCAST, 
        seanode.request_options.FileGeometry.MESH
    )
    # Number of analysis tasks.
    assert len(atn) == 2
    # Test filenames.
    assert atn[0].filename == 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20241201/rerun/stofs_3d_atl.t12z.hrrr.air.nc'
    assert atn[1].filename == 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20241202/rerun/stofs_3d_atl.t12z.hrrr.air.nc'
    # Test coords.
    assert atn[0].coords ==  {'time': 'time', 'latitude': 'lat', 'longitude': 'lon'}
    # Test varlist.
    assert atn[0].varlist == [{'varname_out': 'ps', 'varname_file': 'prmsl', 'datum': None}, 
                              {'varname_out': 'u10', 'varname_file': 'uwind', 'datum': None}, 
                              {'varname_out': 'v10', 'varname_file': 'vwind', 'datum': None}]
    # Test timeslice.
    assert atn[0].timeslice == (datetime.datetime(2024, 12, 1, 12, 0), 
                                datetime.datetime(2024, 12, 1, 12, 0))
    assert atn[1].timeslice == (datetime.datetime(2024, 12, 1, 12, 0), 
                                datetime.datetime(2024, 12, 2, 12, 0))
    # Test file format.
    assert atn[0].file_format == 'nc'
    return None


# ----------------------------------------------------------------    
# Forecast.
def test_get_analysis_task_forecast_current_version():
    atf = hrrr.get_analysis_tasks(
        ['ps', 'u10', 'v10'],
        df_stations,
        dt_start,
        dt_start,
        seanode.request_options.ForecastType.FORECAST, 
        seanode.request_options.FileGeometry.MESH
    )
    # Number of analysis tasks.
    assert len(atf) == 1
    # Test filename.
    assert atf[0].filename == 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20241201/rerun/stofs_3d_atl.t12z.hrrr.air.nc'
    # Test coords.
    assert atf[0].coords == {'time': 'time', 'latitude': 'lat', 'longitude': 'lon'}
    # Test varlist.
    assert atf[0].varlist == [{'varname_out': 'ps', 'varname_file': 'prmsl', 'datum': None}, 
                              {'varname_out': 'u10', 'varname_file': 'uwind', 'datum': None}, 
                              {'varname_out': 'v10', 'varname_file': 'vwind', 'datum': None}]
    # Test timeslice.
    assert atf[0].timeslice == (datetime.datetime(2024, 12, 1, 12, 0), None)
    # Test file format.
    assert atf[0].file_format == 'nc'
    return None


# ----------------------------------------------------------------
# Forecast, unavailable variable.
def test_get_analysis_task_forecast_unavailable_variable():
    atf = hrrr.get_analysis_tasks(
        ['cwl_bias_corrected'],
        df_stations,
        dt_start,
        dt_start,
        seanode.request_options.ForecastType.FORECAST, 
        seanode.request_options.FileGeometry.MESH
    )
    assert atf == []
    return None


# ----------------------------------------------------------------
# Forecast, future start date.
def test_get_analysis_task_forecast_future_start():
    atf = hrrr.get_analysis_tasks(
        ['ps'],
        df_stations,
        dt_future,
        dt_future,
        seanode.request_options.ForecastType.FORECAST, 
        seanode.request_options.FileGeometry.MESH
    )
    # In future, this should return an error.
    assert len(atf) == 1
    assert '20351201' in atf[0].filename
    return None


# ----------------------------------------------------------------
# Forecast, unavailable geometry.
def test_get_analysis_task_forecast_unavailable_geometry(): 
    atf = hrrr.get_analysis_tasks(
        ['ps'],
        df_stations,
        dt_start,
        dt_start,
        seanode.request_options.ForecastType.FORECAST, 
        seanode.request_options.FileGeometry.GRID
    )
    assert atf == []
    return None


# ----------------------------------------------------------------
if __name__ == '__main__':
    test_get_analysis_task_nowcast_current_version()
    test_get_analysis_task_forecast_current_version()
    test_get_analysis_task_forecast_unavailable_variable()
    test_get_analysis_task_forecast_future_start()
    test_get_analysis_task_forecast_unavailable_geometry()
    print("All tests passed.")

