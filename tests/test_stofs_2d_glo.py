import pathlib  
import sys
sys.path.append(str(pathlib.Path(__file__).parent.parent.absolute()))
import datetime
import pandas as pd
import seanode
from seanode.models.stofs_2d_glo import STOFS2DGloTaskCreator
import logging


logging.basicConfig(level=logging.INFO)


s2d = STOFS2DGloTaskCreator()

df_stations = pd.Series(['8720218', '8720357', '8725114']),
dt_start = datetime.datetime(2024,12,1,12,0)
dt_end = datetime.datetime(2024,12,2,12,0)
dt_v2p0 = datetime.datetime(2023,12,1,12,0)
dt_future = datetime.datetime(2035,12,1,12,0)

def test_get_analysis_task_nowcast_current_version():
    # Nowcast, v2.1.
    atn = s2d.get_analysis_tasks(
        ['cwl_bias_corrected', 'u_vel', 'v_vel'],
        df_stations,
        dt_start,
        dt_end,
        seanode.request_options.ForecastType.FORECAST, 
        seanode.request_options.FileGeometry.POINTS
    )
    # Test len
    # Test filenames
    # Test coords
    # Test varlist
    # Test timeslice
    
# Forecast, v2.1
# Forecast, v2.0.
# Forecast, v2.0, unavailable variable.
# Forecast, v2.1, future start date.
# Forecast, v2.1, unavailable geometry.


def test_get_filename():
    fn = s2d.get_filename(
            dt_start, 
            seanode.request_options.FileGeometry.POINTS, 
            'cwl', 'nc'
        )
    assert fn == 'noaa-gestofs-pds/stofs_2d_glo.20241201/stofs_2d_glo.t12z.points.cwl.nc'

