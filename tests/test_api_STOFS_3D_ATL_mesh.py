import pathlib  
import sys
sys.path.append(str(pathlib.Path(__file__).parent.parent.absolute()))
import datetime
import pandas as pd
from seanode.api import get_surge_model_at_stations
import logging


logging.basicConfig(level=logging.INFO)

# Useful for debugging analysis_task_mesh.py:
#import seanode.analysis_task_mesh
#atm_logger = logging.getLogger('seanode.analysis_task_mesh')
#atm_logger.setLevel(logging.DEBUG)

df_stations = pd.DataFrame(
    data={
        'station':['8720218', '8720357', '8447930'],
        'latitude':[30 + (23.9/60.0), 30 + (11.5/60.0), 41 + (31.4/60.0)],
        'longitude':[-81 - (25.7/60.0), -81 - (41.3/60.0), -70 - (40.3/60.0)]
    }
)

df_forecast = get_surge_model_at_stations(
    'STOFS_3D_ATL',
    ['cwl'],
    df_stations,
    datetime.datetime(2024,12,1,12,0),
    None,
    'forecast',
    'mesh',
    'MLLW',
    'AWS'
)

df_nowcast = get_surge_model_at_stations(
    'STOFS_3D_ATL',
    ['cwl'],
    df_stations,
    datetime.datetime(2024,12,1,12,0),
    datetime.datetime(2024,12,2,12,0),
    'nowcast',
    'mesh',
    'MLLW',
    'AWS'
)

assert df_forecast.shape == (288,3), "df_forecast should have shape (288, 3)."
assert df_nowcast.shape == (75,3), "df_nowcast should have shape (75, 3)."