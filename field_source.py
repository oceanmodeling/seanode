"""
"""


from dataclasses import dataclass
from typing import List
from request_options import FileGeometry


@dataclass(frozen=True)
class FieldSource:
    """Define field source object, corresponding to a series of files.

    Parameters
    ----------
    var_group
        Description of the group of variables contained in a file,
        as used in output filenames (e.g. "cwl", "cwl.temp.salt.vel").
        These don't always make total sense.
    file_format
        "nc" for netCDF, "grib2" for GRIB2
    file_geometry
        FileGeometry.POINTS, FileGeometry.MESH, FileGeometry.GRID
    variables
        list of dictionaries, each of the form
        {'varname_out': varname_out, 'varname_file':file_varname, 'datum':datum}
        e.g.,
        {'varname_out': 'cwl_bias_corrected', 'varname_file':'zeta', 'datum':'MSL'}

    Methods
    -------
    """

    var_group: str
    file_format: str
    file_geometry: FileGeometry
    variables: list

    def get_vars(self):
        """
        """
        return [vardict['varname_out'] for vardict in self.variables]




