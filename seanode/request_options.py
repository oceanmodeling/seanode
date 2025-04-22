"""
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

