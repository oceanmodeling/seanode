"""HRRR model and task creator.

Note this version pulls from STOFS-3D-Atl forcing data.

Classes
-------
HRRRTaskCreator

"""


from typing import List, Tuple, Iterable
import pandas
import datetime
from seanode.analysis_task import AnalysisTask
from seanode.analysis_task_mesh import MeshAnalysisTask
from seanode.models.model_task_creator import ModelTaskCreator
from seanode.request_options import FileGeometry, ForecastType
from seanode.field_source import FieldSource


class HRRRTaskCreator(ModelTaskCreator):
    """HRRR model and task creator.

    Extends ModelTaskCreator and get_analysis_tasks, get_init_time_forecast,
    and get_init_times_nowcast methods.
    
    Attributes
    ----------
    bucket_name
    dir_prefix
    file_prefix
    geometry_mapper
    cycles
    nowcast_period
    data_catalog

    Methods
    -------
    get_versions_by_date
    get_field_source
    get_init_time_forecast
    get_init_times_nowcast
    get_analysis_tasks
    get_filename
    
    """

    bucket_name = 'noaa-nos-stofs3d-pds/STOFS-3D-Atl'
    dir_prefix = 'stofs_3d_atl'
    file_prefix = 'stofs_3d_atl'
    cycles = (12,)
    nowcast_period = 24
    data_catalog = {
        'v1':{
            'first_run': datetime.datetime(2021, 3, 22, 12, 0),
            'last_run': None,
            'field_sources':[
                FieldSource('air', 'nc', FileGeometry.MESH,
                            [{'varname_out':'ps', 'varname_file':'prmsl', 'datum':None},
                             {'varname_out':'u10', 'varname_file':'uwind', 'datum':None},
                             {'varname_out':'v10', 'varname_file':'vwind', 'datum':None}],
                            {'time':'time','latitude':'lat','longitude':'lon'})
            ]
        }
    }

    def __init__(self):
        """Initialize GFSTaskCreator.

        Note that this simply initializes the parent class with the 
        GFSTaskCreator class variables.

        Parameters
        ----------
        cycles
            Initilization times of forecast cycles, in whole hours, formatted 
            in an arbitrary length tuple, e.g., (0,12). If the tuple is length
            1, input it like "(1,)", not "(1)".
        nowcast_period
            Length of the nowcast period of a model. This is a STOFS-related
            concept that differentiates a period of a model run before the 
            initialization time, known as a nowcast. Where relevant, the same
            idea can be used for other models (e.g., GFS, used to force STOFS).
        data_catalog
            Describes the available FieldSources of a model, broken down by
            model version.

        """
        super().__init__(self.cycles, self.nowcast_period, self.data_catalog)
    
    def get_analysis_tasks(
        self, 
        request_variables: List[str],
        stations: Iterable | pandas.DataFrame,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        forecast_type: ForecastType,
        geometry: FileGeometry
    ) -> List[AnalysisTask]:
        """Return a list of AnalysisTask objects for this model.

        Parameters
        ----------
        request_variables
            List of variables to retrieve (using their "varname_out"
            values from the data_catalog).
        stations
            A list or other object containing information about the 
            locations at which to retrieve data.
        start_date
            Start of period from which to retrieve data.
        end_date
            End of period from which to retrieve data. Ignored for
            forecast_type = "forecast".
        forecast_type
            Whether to get nowcast or forecast data.
        geometry
            Type of files from which to retrieve data.

        Returns
        -------
        List of analysis tasks for this model.
        
        """
        result = []
        
        # Get versions
        for (version_name, version_start, version_end) in \
            self.get_versions_by_date(start_date, end_date):
            
            # Get initialization datetime(s) and time slices for each.
            if forecast_type == ForecastType.FORECAST:
                # Just a single date for forecast, but formed as a list.
                init_dates, time_slices = self.get_init_time_forecast(
                    version_start
                )
            else:
                # A list of dates for nowcast
                init_dates, time_slices = self.get_init_times_nowcast(
                    version_start, version_end
                )
        
            # Get FieldSources
            fs_list = []
            for var in request_variables:
                fs_new = self.get_field_source(version_name, var, geometry)
                for fs in fs_new:
                    if fs not in fs_list:
                        fs_list.append(fs)
            
            # Create analysis tasks
            for fs in fs_list:
                for idt, dt in enumerate(init_dates):
                    filename = self.get_filename(dt,
                                                 fs.var_group, 
                                                 fs.file_format)
                    task_vars = [var_dict for var_dict in fs.variables 
                                 if var_dict['varname_out'] in request_variables]
                    result.append(
                        MeshAnalysisTask(filename, 
                                         fs.coords,
                                         task_vars,
                                         time_slices[idt],
                                         stations,
                                         fs.file_format)
                        )
        return result
    
    def get_filename(
            self,
            init_datetime: datetime.datetime,
            var_group: str,
            file_format: str
    ) -> str:
        """Get filepath for forecast initialized at a specific time.

        Parameters
        ----------
        init_datetime
            The model run initialization time.
        var_group
            String representing description of file contents.
        file_format
            File format; usually "nc" or "grib2".

        Returns
        -------
        Full path to a single file.
        
        """
        yyyymmdd = init_datetime.strftime('%Y%m%d')
        hh = init_datetime.strftime('%H')
        filepath = f'{self.bucket_name}/{self.dir_prefix}.{yyyymmdd}/rerun/{self.file_prefix}.t{hh}z.hrrr.{var_group}.{file_format}'
        return filepath


        