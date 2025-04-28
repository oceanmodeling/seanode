"""Define options that can be used in model data requests.

These are only used to define the simpler choices available
when making a data request. More open-ended choices (e.g., dates
and station lists/dataframes) are not pre-defined here.

Classes
-------
FileGeometry
ForecastType
ModelOptions
DataStoreOptions

"""


from enum import Enum


class FileGeometry(Enum):
    POINTS = 'points'
    MESH = 'mesh'
    GRID = 'grid'


class ForecastType(Enum):
    NOWCAST = 'nowcast'
    FORECAST = 'forecast'


class ModelOptions(Enum):
    STOFS_2D_GLO = 'stofs_2d_glo'
    STOFS_3D_ATL = 'stofs_3d_atl'
    GFS = 'gfs'


class DataStoreOptions(Enum):
    AWS = 'aws'
    LOCAL = 'local'

