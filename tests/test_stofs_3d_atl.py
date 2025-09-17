import pathlib  
import sys
sys.path.append(str(pathlib.Path(__file__).parent.parent.absolute()))
import datetime
import pandas as pd
import seanode
from seanode.models.stofs_3d_atl import STOFS3DAtlTaskCreator
import logging


logging.basicConfig(level=logging.INFO)


# Get a task creator instance.
s3d = STOFS3DAtlTaskCreator()

# Define some test specifications.
df_stations = pd.Series(['8720218', '8720357', '8725114']),
dt_start = datetime.datetime(2024,12,1,12,0)
dt_end = datetime.datetime(2024,12,2,12,0)
dt_v1p1 = datetime.datetime(2023,12,1,12,0)
dt_future = datetime.datetime(2035,12,1,12,0)


# Define the tests.
# ----------------------------------------------------------------
# Nowcast, v2.1.
def test_get_analysis_task_nowcast_current_version():
    atn = s3d.get_analysis_tasks(
        ['cwl', 'u_vel', 'v_vel'],
        df_stations,
        dt_start,
        dt_end,
        seanode.request_options.ForecastType.NOWCAST, 
        seanode.request_options.FileGeometry.POINTS
    )
    # Test number of analysis tasks.
    assert len(atn) == 4
    # Test filenames.
    assert atn[0].filename == 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20241201/stofs_3d_atl.t12z.points.cwl.nc'
    assert atn[1].filename == 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20241202/stofs_3d_atl.t12z.points.cwl.nc'
    assert atn[2].filename == 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20241201/stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc'
    assert atn[3].filename == 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20241202/stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc'
    # Test coords.
    assert atn[0].coords == {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'}
    assert atn[3].coords == {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'}
    # Test varlist.
    assert atn[0].varlist == [{'varname_out': 'cwl', 'varname_file': 'zeta', 'datum': 'NAVD88'}]
    assert {'varname_out': 'u_vel', 'varname_file': 'u', 'datum': None} in atn[3].varlist
    assert {'varname_out': 'v_vel', 'varname_file': 'v', 'datum': None} in atn[3].varlist
    # Test timeslice.
    assert atn[0].timeslice == (datetime.datetime(2024, 12, 1, 12, 0), 
                                datetime.datetime(2024, 12, 1, 12, 0))
    assert atn[3].timeslice == (datetime.datetime(2024, 12, 1, 12, 0), 
                                datetime.datetime(2024, 12, 2, 12, 0)) 
    return None


# ----------------------------------------------------------------    
# Forecast, v2.1.
def test_get_analysis_task_forecast_current_version():
    atf = s3d.get_analysis_tasks(
        ['cwl', 'u_vel', 'v_vel'],
        df_stations,
        dt_start,
        dt_start,
        seanode.request_options.ForecastType.FORECAST, 
        seanode.request_options.FileGeometry.POINTS
    )
    assert len(atf) == 2
    assert atf[0].filename == 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20241201/stofs_3d_atl.t12z.points.cwl.nc'
    assert atf[1].filename == 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20241201/stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc'
    assert atf[0].coords == {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'}
    assert atf[1].coords == {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'}
    assert atf[0].varlist == [{'varname_out': 'cwl', 'varname_file': 'zeta', 'datum': 'NAVD88'}]
    assert {'varname_out': 'u_vel', 'varname_file': 'u', 'datum': None} in atf[1].varlist
    assert {'varname_out': 'v_vel', 'varname_file': 'v', 'datum': None} in atf[1].varlist
    assert atf[0].timeslice == (datetime.datetime(2024, 12, 1, 12, 0), None)
    assert atf[1].timeslice == (datetime.datetime(2024, 12, 1, 12, 0), None)
    return None


# ----------------------------------------------------------------
# Forecast, v1.1.
def test_get_analysis_task_forecast_v1p1():
    atf = s3d.get_analysis_tasks(
        ['cwl', 'u_vel', 'v_vel'],
        df_stations,
        dt_v1p1,
        dt_v1p1,
        seanode.request_options.ForecastType.FORECAST, 
        seanode.request_options.FileGeometry.POINTS
    )
    assert len(atf) == 2
    assert atf[0].filename == 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20231201/stofs_3d_atl.t12z.points.cwl.nc'
    assert atf[1].filename == 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20231201/stofs_3d_atl.t12z.points.cwl.temp.salt.vel.nc'
    assert atf[0].coords == {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'}
    assert atf[1].coords == {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'}
    assert atf[0].varlist == [{'varname_out': 'cwl', 'varname_file': 'zeta', 'datum': 'NAVD88'}]
    assert {'varname_out': 'u_vel', 'varname_file': 'u', 'datum': None} in atf[1].varlist
    assert {'varname_out': 'v_vel', 'varname_file': 'v', 'datum': None} in atf[1].varlist
    assert atf[0].timeslice == (datetime.datetime(2023, 12, 1, 12, 0), None)
    assert atf[1].timeslice == (datetime.datetime(2023, 12, 1, 12, 0), None)
    return None


# ----------------------------------------------------------------
# Forecast, v1.1, unavailable variable.
def test_get_analysis_task_forecast_v1p1_unavailable_variable():
    try:
        s3d.get_analysis_tasks(
            ['cwl_bias_corrected'],
            df_stations,
            dt_v1p1,
            dt_v1p1,
            seanode.request_options.ForecastType.FORECAST, 
            seanode.request_options.FileGeometry.POINTS
        )
    except Exception as e:
        assert "'NoneType' object is not iterable" in str(e)
    return None


# ----------------------------------------------------------------
# Forecast, v2.1, future start date.
def test_get_analysis_task_forecast_future_start():
    atf = s3d.get_analysis_tasks(
        ['cwl'],
        df_stations,
        dt_future,
        dt_future,
        seanode.request_options.ForecastType.FORECAST, 
        seanode.request_options.FileGeometry.POINTS
    )
    # In future, this should return an error.
    assert len(atf) == 1
    assert '20351201' in atf[0].filename
    return None


# ----------------------------------------------------------------
# Forecast, v2.1, unavailable geometry.
def test_get_analysis_task_forecast_unavailable_geometry():
    try:
        s3d.get_analysis_tasks(
            ['cwl'],
            df_stations,
            dt_start,
            dt_end,
            seanode.request_options.ForecastType.FORECAST, 
            seanode.request_options.FileGeometry.GRID
        )
    except TypeError as e:
        assert "'NoneType' object is not iterable" in str(e)
        return None

# ----------------------------------------------------------------
# Filename generation.
def test_get_filename():
    fn = s3d.get_filename(
            dt_start, 
            seanode.request_options.FileGeometry.POINTS, 
            'cwl', 'nc'
        )
    assert fn == 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20241201/stofs_3d_atl.t12z.points.cwl.nc'


if __name__ == '__main__':
    test_get_analysis_task_nowcast_current_version()
    test_get_analysis_task_forecast_current_version()
    test_get_analysis_task_forecast_v1p1()
    test_get_analysis_task_forecast_v1p1_unavailable_variable()
    test_get_analysis_task_forecast_future_start()
    test_get_analysis_task_forecast_unavailable_geometry()
    test_get_filename()
    print("All tests passed.")

