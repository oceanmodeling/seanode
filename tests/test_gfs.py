import pathlib  
import sys
sys.path.append(str(pathlib.Path(__file__).parent.parent.absolute()))
import datetime
import pandas as pd
import seanode
from seanode.models.gfs import GFSTaskCreator
import logging


logging.basicConfig(level=logging.INFO)


# Get a task creator instance.
gfs = GFSTaskCreator()

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
    atn = gfs.get_analysis_tasks(
        ['ps', 'u10', 'v10'],
        df_stations,
        dt_start,
        dt_end,
        seanode.request_options.ForecastType.NOWCAST, 
        seanode.request_options.FileGeometry.GRID
    )
    # Number of analysis tasks.
    assert len(atn) == 1
    # Test number of analysis task files (all in one analysis task).
    # (Number of variables) * (Number of hours [remember inclusive]) 
    assert len(atn[0].filename) == 75
    # Test filenames.
    for fn in [
            'noaa-gfs-bdp-pds/gfs.20241201/12/atmos/gfs.t12z.sfluxgrbf000_message0.json',
            'noaa-gfs-bdp-pds/gfs.20241201/12/atmos/gfs.t12z.sfluxgrbf000_message1.json',
            'noaa-gfs-bdp-pds/gfs.20241201/12/atmos/gfs.t12z.sfluxgrbf000_message2.json',
            'noaa-gfs-bdp-pds/gfs.20241202/00/atmos/gfs.t00z.sfluxgrbf005_message0.json',
            'noaa-gfs-bdp-pds/gfs.20241202/12/atmos/gfs.t12z.sfluxgrbf000_message0.json'
        ]:
        assert any(fn in fnf for fnf in atn[0].filename)
    # Test coords.
    assert atn[0].coords == {'time': 'valid_time', 'init_time': 'time', 'latitude': 'latitude', 'longitude': 'longitude'}
    # Test varlist.
    assert atn[0].varlist == [{'varname_out': 'ps', 'varname_file': 'sp', 'datum': None}, 
                              {'varname_out': 'u10', 'varname_file': 'u10', 'datum': None}, 
                              {'varname_out': 'v10', 'varname_file': 'v10', 'datum': None}]
    # Test timeslice.
    assert atn[0].timeslice is None
    # Test file format.
    assert atn[0].file_format == 'kerchunk'
    return None


# ----------------------------------------------------------------    
# Forecast.
def test_get_analysis_task_forecast_current_version():
    atf = gfs.get_analysis_tasks(
        ['ps', 'u10', 'v10'],
        df_stations,
        dt_start,
        dt_start,
        seanode.request_options.ForecastType.FORECAST, 
        seanode.request_options.FileGeometry.GRID
    )
    # Number of analysis tasks.
    assert len(atf) == 1
    # Test number of analysis task files (all in one analysis task).
    # (Number of variables) * (Number of lead times in forecast)
    # Hourly from 0 to 119 (=120 lead times), 
    # then 3-hourly from 120 to 180 (=21 lead times). 
    assert len(atf[0].filename) == 423
    # Test filenames.
    for fn in [
            'noaa-gfs-bdp-pds/gfs.20241201/12/atmos/gfs.t12z.sfluxgrbf000_message0.json',
            'noaa-gfs-bdp-pds/gfs.20241201/12/atmos/gfs.t12z.sfluxgrbf000_message1.json',
            'noaa-gfs-bdp-pds/gfs.20241201/12/atmos/gfs.t12z.sfluxgrbf000_message2.json',
            'noaa-gfs-bdp-pds/gfs.20241201/12/atmos/gfs.t12z.sfluxgrbf119_message0.json',
            'noaa-gfs-bdp-pds/gfs.20241201/12/atmos/gfs.t12z.sfluxgrbf120_message0.json',
            'noaa-gfs-bdp-pds/gfs.20241201/12/atmos/gfs.t12z.sfluxgrbf123_message0.json',
            'noaa-gfs-bdp-pds/gfs.20241201/12/atmos/gfs.t12z.sfluxgrbf180_message2.json'
        ]:
        assert any(fn in fnf for fnf in atf[0].filename)
    # Test coords.
    assert atf[0].coords == {'time': 'valid_time', 'init_time': 'time', 'latitude': 'latitude', 'longitude': 'longitude'}
    # Test varlist.
    assert atf[0].varlist == [{'varname_out': 'ps', 'varname_file': 'sp', 'datum': None}, 
                              {'varname_out': 'u10', 'varname_file': 'u10', 'datum': None}, 
                              {'varname_out': 'v10', 'varname_file': 'v10', 'datum': None}]
    # Test timeslice.
    assert atf[0].timeslice is None
    # Test file format.
    assert atf[0].file_format == 'kerchunk'
    return None


# ----------------------------------------------------------------
# Forecast, unavailable variable.
def test_get_analysis_task_forecast_unavailable_variable():
    try:
        gfs.get_analysis_tasks(
            ['cwl_bias_corrected'],
            df_stations,
            dt_start,
            dt_start,
            seanode.request_options.ForecastType.FORECAST, 
            seanode.request_options.FileGeometry.GRID
        )
    except Exception as e:
        assert "'NoneType' object is not iterable" in str(e)
    return None


# ----------------------------------------------------------------
# Forecast, future start date.
def test_get_analysis_task_forecast_future_start():
    try:
        atf = gfs.get_analysis_tasks(
            ['ps'],
            df_stations,
            dt_future,
            dt_future,
            seanode.request_options.ForecastType.FORECAST, 
            seanode.request_options.FileGeometry.GRID
        )
    except FileNotFoundError as e:
        assert "noaa-gfs-bdp-pds/gfs.20351201/12/atmos/gfs.t12z.sfluxgrb" in str(e)
        return None


# ----------------------------------------------------------------
# Forecast, unavailable geometry.
def test_get_analysis_task_forecast_unavailable_geometry():
    try:
        gfs.get_analysis_tasks(
            ['ps'],
            df_stations,
            dt_start,
            dt_start,
            seanode.request_options.ForecastType.FORECAST, 
            seanode.request_options.FileGeometry.POINTS
        )
    except TypeError as e:
        assert "'NoneType' object is not iterable" in str(e)
        return None


# ----------------------------------------------------------------
# Forecast initialization time determination.
def test_get_init_time_forecast():
    # Case 1: start_date falls exactly on a forecast initialization time (e.g., 12Z)
    dt_on_cycle = datetime.datetime(2024, 12, 1, 12, 0)
    init_dates, lead_times = gfs.get_init_time_forecast(dt_on_cycle)
    assert len(init_dates) == 1
    assert init_dates[0] == dt_on_cycle
    assert lead_times[0][0] == 0
    assert lead_times[0][-1] == 180

    # Case 2: start_date does NOT fall on a forecast initialization time (e.g., 15Z, next cycle is 18Z)
    dt_off_cycle = datetime.datetime(2024, 12, 1, 15, 0)
    init_dates, lead_times = gfs.get_init_time_forecast(dt_off_cycle)
    # Should return the latest cycle before or equal to 15Z, which is 12Z
    assert len(init_dates) == 1
    assert init_dates[0] == datetime.datetime(2024, 12, 1, 12, 0)
    assert lead_times[0][0] == 0
    assert lead_times[0][-1] == 180
    return None


# ----------------------------------------------------------------
# Nowcast initialization time determination.
def test_get_init_times_nowcast():
    # Case 1: start_date and end_date both fall exactly on initialization times
    start1 = datetime.datetime(2024, 12, 1, 6, 0)
    end1 = datetime.datetime(2024, 12, 1, 18, 0)
    inits1, leads1 = gfs.get_init_times_nowcast(start1, end1)
    assert inits1 == [datetime.datetime(2024, 12, 1, 6, 0), 
                      datetime.datetime(2024, 12, 1, 12, 0), 
                      datetime.datetime(2024, 12, 1, 18, 0)]
    assert leads1 == [(0, 1, 2, 3, 4, 5), 
                      (0, 1, 2, 3, 4, 5), 
                      (0,)]

    # Case 2: start_date on init, end_date not on init
    start2 = datetime.datetime(2024, 12, 1, 12, 0)
    end2 = datetime.datetime(2024, 12, 1, 19, 30)
    inits2, leads2 = gfs.get_init_times_nowcast(start2, end2)
    assert inits2 == [datetime.datetime(2024, 12, 1, 12, 0), 
                      datetime.datetime(2024, 12, 1, 18, 0)]
    assert leads2 == [(0, 1, 2, 3, 4, 5), 
                      (0, 1, 2)]

    # Case 3: start_date not on init, end_date on init
    start3 = datetime.datetime(2024, 12, 1, 7, 15)
    end3 = datetime.datetime(2024, 12, 1, 12, 0)
    inits3, leads3 = gfs.get_init_times_nowcast(start3, end3)
    assert inits3 == [datetime.datetime(2024, 12, 1, 6, 0), 
                      datetime.datetime(2024, 12, 1, 12, 0)]
    assert leads3 == [(1, 2, 3, 4, 5), 
                      (0,)]

    # Case 4: neither start_date nor end_date on init
    start4 = datetime.datetime(2024, 12, 1, 6, 15)
    end4 = datetime.datetime(2024, 12, 1, 13, 30)
    inits4, leads4 = gfs.get_init_times_nowcast(start4, end4)
    assert inits4 == [datetime.datetime(2024, 12, 1, 6, 0), 
                      datetime.datetime(2024, 12, 1, 12, 0)]
    assert leads4 == [(0, 1, 2, 3, 4, 5), 
                      (0, 1, 2)]
    return None


# ----------------------------------------------------------------
if __name__ == '__main__':
    test_get_analysis_task_nowcast_current_version()
    test_get_analysis_task_forecast_current_version()
    test_get_analysis_task_forecast_unavailable_variable()
    test_get_analysis_task_forecast_future_start()
    test_get_analysis_task_forecast_unavailable_geometry()
    test_get_init_time_forecast()
    test_get_init_times_nowcast()
    print("All tests passed.")

