"""
"""


import sys
from surgestations.request_options import ModelOptions
from surgestations.models.stofs_2d_glo import STOFS2DGloTaskCreator


def get_model(model_name):
    """
    """
    if model_name == ModelOptions.STOFS_2D_GLO:
        return STOFS2DGloTaskCreator()
    elif model_name == ModelOptions.STOFS_3D_ATL:
        return STOFS3DAtlTaskCreator()
    else:
        sys.exit(f'model_name {model_name} not recognized.')