"""Field source class definition, describing format/contents of a series of files.

Classes
-------
FieldSource

"""


from dataclasses import dataclass
from typing import List
from seanode.request_options import FileGeometry


@dataclass(frozen=True)
class FieldSource:
    """Define field source object, corresponding to a series of files.

    Future: Maybe this is where the get_filename(...) methods should go,
    using this kind of syntax:
    https://stackoverflow.com/a/44757255

    Attributes
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
    coords
        dictionary mapping a file's coordinates to standard names, in the 
        pattern {standard_name: name_in_file}
        E.g.,
        {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'}

    Methods
    -------
    get_vars()
        Returns a list of varname_out values for all entries in variables.
        
    """

    var_group: str
    file_format: str
    file_geometry: FileGeometry
    variables: list
    coords: dict

    def get_vars(self) -> List[str]:
        """Return list of output variable names available in a FieldSource."""
        return [vardict['varname_out'] for vardict in self.variables]




