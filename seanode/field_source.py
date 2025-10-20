"""Field source class definition, describing format/contents of a series of files.

Classes
-------
FieldSource

"""


from dataclasses import dataclass
import datetime
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
    filename_template
        A python format string defining the naming pattern of the files,
        e.g., 'cwl_{yyyymmdd}_{hh}z.nc'
        This can include string formatting, e.g., {forecast_lead:03d}.
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
    file_geometry
        FileGeometry.POINTS, FileGeometry.MESH, FileGeometry.GRID
    file_format
        "nc" for netCDF, "grib2" for GRIB2.
        Could also be "kerchunk" for the special case where
        other files (e.g., GRIB2) are referenced via a kerchunk JSON file.
        In this case, the filename_template points to the underlying file,
        and will have a different extension than "kerchunk".

    Methods
    -------
    get_vars()
        Returns a list of varname_out values for all entries in variables.
    get_filename(init_datetime, **namespace)
        Returns a filename string for a given initialization datetime and
        optional namespace arguments. By default, the init_datetime
        is used to create 'yyyymmdd' and 'hh' values for use in the
        filename_template. Any other filename components must be passed
        as keyword arguments in the namespace.
        
    """

    filename_template: str
    variables: list
    coords: dict
    file_geometry: FileGeometry
    file_format: str

    def get_vars(self) -> List[str]:
        """Return list of output variable names available in a FieldSource."""
        return [vardict['varname_out'] for vardict in self.variables]
    

    def get_filename(self, 
                     init_datetime: datetime.datetime | None, 
                     namespace: dict = {}) -> str:
        """Return a filename string for a given initialization datetime...

        and optional namespace arguments. By default, the init_datetime
        is used to create 'yyyymmdd' and 'hh' values for use in the
        filename_template. Any other filename components must be passed
        as keyword arguments in the namespace.

        Parameters
        ----------
        init_datetime : datetime.datetime or None
            Initialization datetime for the desired file.
            Used to create 'yyyymmdd' and 'hh' values.
        namespace : dict
            Any other keyword arguments needed to fill in the
            filename_template. Any values not used by the 
            filename_template are ignored. 

        Returns
        -------
        str
            The filename string.

        """
        if init_datetime:
            dt_dict = {
                'yyyymmdd': init_datetime.strftime('%Y%m%d'),
                'hh': init_datetime.strftime('%H')
            }
        else:
            dt_dict = {}
        result = self.filename_template.format(
            **dt_dict,
            **namespace
        )
        return result


