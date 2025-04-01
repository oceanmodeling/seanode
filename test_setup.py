import datetime
import pandas
import xarray
import numpy
#
from seanode.api import get_surge_model_at_stations

"""
df_out = get_surge_model_at_stations(
    'STOFS_2D_GLO',
    ['cwl_bias_corrected', 'u_vel', 'v_vel'],
    pandas.Series(['8720218', '8720357', '8725114']),
    datetime.datetime(2024,12,1,12,0),
    None,
    'forecast',
    'points',
    'MLLW',
    'AWS'
)

print(df_out)
"""

# Mesh analysis task test.
from seanode.mesh_analysis_task import MeshAnalysisTask
from seanode.data_stores import AWSDataStore

store = AWSDataStore()
task = MeshAnalysisTask('noaa-gestofs-pds/stofs_2d_glo.20241201/stofs_2d_glo.t00z.fields.cwl.nc', [{'varname_out':'cwl_raw', 'varname_file':'zeta', 'datum':'LMSL'}], None, pandas.DataFrame(data={'latitude':numpy.array([30.3, 30.4, 30.5, 30.6, 30.7]), "longitude":numpy.array([-88.0, -88., -88., -88., -88.])}))

ds = task.open_dataset(store)
df = task.get_subset(ds)
