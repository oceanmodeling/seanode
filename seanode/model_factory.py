"""Factory to return a model object.

Functions
---------
get_model

"""


import sys
from seanode.request_options import ModelOptions
from seanode.models.model_task_creator import ModelTaskCreator
from seanode.models.stofs_2d_glo import STOFS2DGloTaskCreator
from seanode.models.stofs_3d_atl import STOFS3DAtlTaskCreator
from seanode.models.gfs import GFSTaskCreator
from seanode.models.hrrr import HRRRTaskCreator


def get_model(model_name: ModelOptions) -> ModelTaskCreator:
    """Return a ModelTaskCreator given a model name."""
    if model_name == ModelOptions.STOFS_2D_GLO:
        return STOFS2DGloTaskCreator()
    elif model_name == ModelOptions.STOFS_3D_ATL:
        return STOFS3DAtlTaskCreator()
    elif model_name == ModelOptions.GFS:
        return GFSTaskCreator()
    elif model_name == ModelOptions.HRRR:
        return HRRRTaskCreator()
    else:
        raise ValueError(f'model_name {model_name} not recognized.')