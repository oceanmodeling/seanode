"""Test ModelTaskCreator functionality.

The following tests are included:
test_get_analysis_tasks: Test that get_analysis_tasks() raises NotImplementedError.
test_get_versions_by_date: Test that get_versions_by_date() returns expected versions.
test_get_field_source: Test that get_field_source() returns expected FieldSource objects.
test_get_init_time_forecast: Test that get_init_time_forecast() returns expected init times and time slices.
test_get_init_times_nowcast: Test that get_init_times_nowcast() returns expected init times and time slices.

"""


import pathlib  
import sys
import datetime
import pandas as pd
sys.path.append(str(pathlib.Path(__file__).parent.parent.absolute()))
from seanode.models.model_task_creator import ModelTaskCreator
from seanode.field_source import FieldSource
from seanode.request_options import FileGeometry
import logging


logging.basicConfig(level=logging.INFO)


# Instantiate a dummy ModelTaskCreator for testing.
# Uses a minimal data catalog based on STOFS_2D_GLO.
cycles = (0, 6, 12, 18)
nowcast_period = 6
data_catalog = {
    'v2.1':{
        'first_run': datetime.datetime(2024, 5, 14, 12, 0),
        'last_run': None,
        'field_sources':[
            FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.points.cwl.nc',
                        [{'varname_out':'cwl_bias_corrected', 'varname_file':'zeta', 'datum':'LMSL'}],
                        {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                        FileGeometry.POINTS, 'nc'),
            FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.points.cwl.noanomaly.nc',
                        [{'varname_out':'cwl_raw', 'varname_file':'zeta', 'datum':'LMSL'}],
                        {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                        FileGeometry.POINTS, 'nc')
        ]
    },
    'v2.0':{
        'first_run':datetime.datetime(2020, 12, 30, 0, 0),
        'last_run': datetime.datetime(2024, 5, 14, 6, 0),
        'field_sources':[
            FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.points.cwl.nc',
                        [{'varname_out':'cwl_raw', 'varname_file':'zeta', 'datum':'LMSL'}],
                        {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                        FileGeometry.POINTS, 'nc')
        ]
    }
}
mtc = ModelTaskCreator(cycles, nowcast_period, data_catalog)


# Define the test functions.
def test_get_analysis_tasks():
    try:
        mtc.get_analysis_tasks()
    except NotImplementedError:
        assert True
    else:
        assert False, "Expected NotImplementedError was not raised."

def test_get_versions_by_date():
    versions = mtc.get_versions_by_date(datetime.datetime(2024,6,1,12,0), datetime.datetime(2024,6,30,12,0))
    assert versions == [('v2.1', datetime.datetime(2024,6,1,12,0), datetime.datetime(2024,6,30,12,0))], f"Unexpected versions: {versions}"
    
    versions = mtc.get_versions_by_date(datetime.datetime(2024,5,10,12,0), datetime.datetime(2024,5,20,12,0))
    assert ('v2.0',datetime.datetime(2024,5,10,12,0),datetime.datetime(2024,5,14,6,0)) in versions and ('v2.1', datetime.datetime(2024,5,14,12,0), datetime.datetime(2024,5,20,12,0)) in versions, f"Unexpected versions: {versions}"
    
    versions = mtc.get_versions_by_date(datetime.datetime(2020,1,1,12,0), datetime.datetime(2020,1,31,12,0))
    assert versions == [], f"Unexpected versions: {versions}"

def test_get_field_source():
    fs_list = mtc.get_field_source('v2.1', 'cwl_bias_corrected', FileGeometry.POINTS)
    assert len(fs_list) == 1, f"Expected 1 FieldSource, got {len(fs_list)}"
    assert fs_list[0].variables[0]['varname_out'] == 'cwl_bias_corrected', "Variable name mismatch."

    fs_list = mtc.get_field_source('v2.0', 'cwl_bias_corrected', 'points')
    assert len(fs_list) == 0, f"Expected 0 FieldSource, got {len(fs_list)}"

def test_get_init_time_forecast():
    # Case 1: start_date falls exactly on a forecast initialization time (e.g., 12Z)
    dt_on_cycle = datetime.datetime(2024, 12, 1, 12, 0)
    init_times, time_slices = mtc.get_init_time_forecast(dt_on_cycle)
    assert len(init_times) == 1, f"Expected 1 init time, got {len(init_times)}"
    assert init_times[0] == dt_on_cycle, "Init time mismatch."
    assert time_slices[0][0] == dt_on_cycle, "Time slice mismatch."
    assert time_slices[0][1] is None, "End of time slice should be None for forecast."

    # Case 2: start_date falls between two forecast initialization times (e.g., 15Z, between 12Z and 18Z)
    dt_between_cycles = datetime.datetime(2024, 12, 1, 15, 0)
    init_times, time_slices = mtc.get_init_time_forecast(dt_between_cycles)
    assert len(init_times) == 1, f"Expected 1 init time, got {len(init_times)}"
    expected_init = datetime.datetime(2024, 12, 1, 12, 0)
    assert init_times[0] == expected_init, "Init time mismatch."
    assert time_slices[0][0] == expected_init, "Time slice mismatch."
    assert time_slices[0][1] is None, "End of time slice should be None for forecast."

def test_get_init_times_nowcast():
    # Case 1: start_date and end_date both fall exactly on initialization times
    dt_start_on_cycle = datetime.datetime(2024, 12, 1, 12, 0)
    dt_end_on_cycle = datetime.datetime(2024, 12, 2, 0, 0)
    init_times, time_slices = mtc.get_init_times_nowcast(dt_start_on_cycle, dt_end_on_cycle)
    expected_inits = [datetime.datetime(2024, 12, 1, 12, 0), 
                      datetime.datetime(2024, 12, 1, 18, 0),
                      datetime.datetime(2024, 12, 2, 0, 0)]
    expected_slices = [(datetime.datetime(2024, 12, 1, 12, 0), datetime.datetime(2024, 12, 1, 12, 0)),
                       (datetime.datetime(2024, 12, 1, 12, 0), datetime.datetime(2024, 12, 1, 18, 0)),
                       (datetime.datetime(2024, 12, 1, 18, 0), datetime.datetime(2024, 12, 2, 0, 0))]
    assert init_times == expected_inits, f"Init times mismatch: {init_times}"
    assert time_slices == expected_slices, f"Time slices mismatch: {time_slices}"

    # Case 2: start_date on init, end_date not on init
    dt_start_on_cycle = datetime.datetime(2024, 12, 1, 12, 0)
    dt_end_off_cycle = datetime.datetime(2024, 12, 2, 3, 0)
    init_times, time_slices = mtc.get_init_times_nowcast(dt_start_on_cycle, dt_end_off_cycle)
    expected_inits = [datetime.datetime(2024, 12, 1, 12, 0), 
                      datetime.datetime(2024, 12, 1, 18, 0),
                      datetime.datetime(2024, 12, 2, 0, 0),
                      datetime.datetime(2024, 12, 2, 6, 0)]
    expected_slices = [(datetime.datetime(2024, 12, 1, 12, 0), datetime.datetime(2024, 12, 1, 12, 0)),
                       (datetime.datetime(2024, 12, 1, 12, 0), datetime.datetime(2024, 12, 1, 18, 0)),
                       (datetime.datetime(2024, 12, 1, 18, 0), datetime.datetime(2024, 12, 2, 0, 0)),
                       (datetime.datetime(2024, 12, 2, 0, 0), datetime.datetime(2024, 12, 2, 3, 0))]
    assert init_times == expected_inits, f"Init times mismatch: {init_times}"
    assert time_slices == expected_slices, f"Time slices mismatch: {time_slices}"

    # Case 3: start_date not on init, end_date on init
    dt_start_off_cycle = datetime.datetime(2024, 12, 1, 14, 0)
    dt_end_on_cycle = datetime.datetime(2024, 12, 2, 0, 0)
    init_times, time_slices = mtc.get_init_times_nowcast(dt_start_off_cycle, dt_end_on_cycle)
    expected_inits = [datetime.datetime(2024, 12, 1, 18, 0),
                      datetime.datetime(2024, 12, 2, 0, 0)]
    expected_slices = [(datetime.datetime(2024, 12, 1, 14, 0), datetime.datetime(2024, 12, 1, 18, 0)),
                       (datetime.datetime(2024, 12, 1, 18, 0), datetime.datetime(2024, 12, 2, 0, 0))]
    assert init_times == expected_inits, f"Init times mismatch: {init_times}"
    assert time_slices == expected_slices, f"Time slices mismatch: {time_slices}"

    # Case 4: neither start_date nor end_date on init
    dt_start_off_cycle = datetime.datetime(2024, 12, 1, 14, 0)
    dt_end_off_cycle = datetime.datetime(2024, 12, 2, 2, 0)
    init_times, time_slices = mtc.get_init_times_nowcast(dt_start_off_cycle, dt_end_off_cycle)
    expected_inits = [datetime.datetime(2024, 12, 1, 18, 0),
                      datetime.datetime(2024, 12, 2, 0, 0),
                      datetime.datetime(2024, 12, 2, 6, 0)]
    expected_slices = [(datetime.datetime(2024, 12, 1, 14, 0), datetime.datetime(2024, 12, 1, 18, 0)),
                       (datetime.datetime(2024, 12, 1, 18, 0), datetime.datetime(2024, 12, 2, 0, 0)),
                       (datetime.datetime(2024, 12, 2, 0, 0), datetime.datetime(2024, 12, 2, 2, 0))]
    assert init_times == expected_inits, f"Init times mismatch: {init_times}"
    assert time_slices == expected_slices, f"Time slices mismatch: {time_slices}"

# Run the tests.
if __name__ == "__main__":
    test_get_analysis_tasks()
    test_get_versions_by_date()
    test_get_field_source()
    test_get_init_time_forecast()
    test_get_init_times_nowcast()
    print("All tests passed.")