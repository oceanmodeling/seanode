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
        'station':['1612340', '9447130', '9462620'],
        'latitude':[21.295071, 47.610207, 53.879111],
        'longitude':[-157.875113, -122.389801, -166.542221]
    }
)

df_forecast = get_surge_model_at_stations(
    'STOFS_3D_PAC',
    ['cwl'],
    df_stations,
    datetime.datetime(2026,2,1,12,0),
    None,
    'forecast',
    'mesh',
    'MLLW',
    'AWS'
)

df_nowcast = get_surge_model_at_stations(
    'STOFS_3D_PAC',
    ['cwl'],
    df_stations,
    datetime.datetime(2026,2,12,3,0),
    datetime.datetime(2026,2,13,21,0),
    'nowcast',
    'mesh',
    'MLLW',
    'AWS'
)

assert df_forecast.shape == (144,3), "df_forecast should have shape (144, 3)."
assert df_nowcast.shape == (129,3), "df_nowcast should have shape (129, 3)."

print('---------- df_forecast ----------')
print(df_forecast)
print('---------- df_nowcast ----------')
print(df_nowcast)