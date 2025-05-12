import xarray
import xarray as xr
import pandas 
import pandas as pd
import scipy
import numpy
import numpy as np
import logging
from seanode.data_stores import AWSDataStore
from seanode.analysis_task_mesh import (
    MeshAnalysisTask, 
    get_nearest_dists_inds, 
    calc_inv_dist_wts
)


logging.basicConfig(level=logging.INFO)


df_stations = pd.DataFrame(
    data={
        'station':['8720218', '8720357', '8447930'],
        'latitude':[30 + (23.9/60.0), 30 + (11.5/60.0), 41 + (31.4/60.0)],
        'longitude':[-81 - (25.7/60.0), -81 - (41.3/60.0), -70 - (40.3/60.0)]
    }
)

store = AWSDataStore()

hrrr_grid_fn = 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20240515/rerun/stofs_3d_atl.t12z.hrrr.air.nc'
hrrr_mesh_fn = 'noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.20240515/schout_adcirc_20240514.nc'

task = MeshAnalysisTask(
    hrrr_grid_fn,
    {'latitude':'lat', 'longitude':'lon', 'time':'time'},
    [{'varname_out':'ps', 'varname_file':'prmsl', 'datum':None},
     {'varname_out':'u10', 'varname_file':'uwind', 'datum':None},
     {'varname_out':'v10', 'varname_file':'vwind', 'datum':None}],
    None,
    df_stations,
    'nc'
)

result = task.run(store, None)

