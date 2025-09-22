"""STOFS-3D-Atlantic model and task creator.

Classes
-------
STOFS3DAtlTaskCreator


"""


# External libraries
from typing import List, Tuple, Iterable
import datetime
import pandas
# This package
from seanode.analysis_task import AnalysisTask, STOFS3DAtlAnalysisTask
from seanode.analysis_task_mesh import MeshAnalysisTask
from seanode.models.model_task_creator import ModelTaskCreator
from seanode.request_options import FileGeometry, ForecastType
from seanode.field_source import FieldSource


class STOFS3DAtlTaskCreator(ModelTaskCreator):
    """STOFS-3D-Atlantic model and task creator.

    Extends ModelTaskCreator and get_analysis_tasks method.
    
    Attributes
    ----------
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
    _get_point_analysis_tasks
    _get_mesh_analysis_tasks
    
    """

    cycles = (12,)
    nowcast_period = 24
    data_catalog = {
        'v2.1':{
            'first_run': datetime.datetime(2024, 5, 14, 12, 0),
            'last_run': None,
            'field_sources':[
                FieldSource('noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.{yyyymmdd}/stofs_3d_atl.t{hh}z.points.cwl.nc', 
                            [{'varname_out':'cwl', 'varname_file':'zeta', 'datum':'NAVD88'}],
                            {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                            FileGeometry.POINTS, 'nc'),
                FieldSource('noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.{yyyymmdd}/stofs_3d_atl.t{hh}z.points.cwl.temp.salt.vel.nc',
                            [{'varname_out':'temperature', 'varname_file':'temperature', 'datum':None},
                             {'varname_out':'salinity', 'varname_file':'salinity', 'datum':None},
                             {'varname_out':'u_vel', 'varname_file':'u', 'datum':None},
                             {'varname_out':'v_vel', 'varname_file':'v', 'datum':None}],
                            {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                            FileGeometry.POINTS, 'nc'),
                FieldSource('noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.{yyyymmdd}/stofs_3d_atl.t{hh}z.fields.out2d_{file_hour}.nc',
                            [{'varname_out':'cwl', 'varname_file':'elevation', 'datum':'xGEOID20B'}],
                            {'latitude':'SCHISM_hgrid_node_y', 'longitude':'SCHISM_hgrid_node_x', 'time':'time'},
                            FileGeometry.MESH, 'nc')
            ]
        },
        'v1.1':{
            'first_run':datetime.datetime(2023, 1, 12, 12, 0),
            'last_run': datetime.datetime(2024, 5, 13, 12, 0),
            'field_sources':[
                FieldSource('noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.{yyyymmdd}/stofs_3d_atl.t{hh}z.points.cwl.nc',
                            [{'varname_out':'cwl', 'varname_file':'zeta', 'datum':'NAVD88'}],
                            {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                            FileGeometry.POINTS, 'nc'),
                FieldSource('noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.{yyyymmdd}/stofs_3d_atl.t{hh}z.points.cwl.temp.salt.vel.nc',
                            [{'varname_out':'temperature', 'varname_file':'temperature', 'datum':None},
                             {'varname_out':'salinity', 'varname_file':'salinity', 'datum':None},
                             {'varname_out':'u_vel', 'varname_file':'u', 'datum':None},
                             {'varname_out':'v_vel', 'varname_file':'v', 'datum':None}],
                            {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                            FileGeometry.POINTS, 'nc'),
                FieldSource('noaa-nos-stofs3d-pds/STOFS-3D-Atl/stofs_3d_atl.{yyyymmdd}/stofs_3d_atl.t{hh}z.fields.out2d_{file_hour}.nc',
                            [{'varname_out':'cwl', 'varname_file':'elevation', 'datum':'xGEOID20B'}],
                            {'latitude':'SCHISM_hgrid_node_y', 'longitude':'SCHISM_hgrid_node_x', 'time':'time'},
                            FileGeometry.MESH, 'nc')
            ]
        }
    }

    def __init__(self):
        """Initialize STOFS3DAtlTaskCreator.

        Note that this simply initializes the parent class with the 
        STOFS3DAtlTaskCreator class variables.

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
        if geometry == FileGeometry.POINTS:
            return self._get_point_analysis_tasks(
                request_variables,
                stations,
                start_date,
                end_date,
                forecast_type
            )
        elif geometry == FileGeometry.MESH:
            return self._get_mesh_analysis_tasks(
                request_variables,
                stations,
                start_date,
                end_date,
                forecast_type
            )
        else:
            raise ValueError("geometry must be FileGeometry.POINTS or FileGeometry.MESH.")
        
    def _get_point_analysis_tasks(
        self, 
        request_variables: List[str],
        stations: Iterable | pandas.DataFrame,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        forecast_type: ForecastType
    ) -> List[AnalysisTask]:
        """Return a list of (point) AnalysisTask objects for this model.

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

        Returns
        -------
        List of analysis tasks for this model.
        
        """ 
        result = []
        # Get versions
        for (version_name, version_start, version_end) in \
            self.get_versions_by_date(start_date, end_date):
            
            # Get date(s)
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
                fs_new = self.get_field_source(version_name, var, 
                                               FileGeometry.POINTS)
                for fs in fs_new:
                    if fs not in fs_list:
                        fs_list.append(fs)
            
            # Create analysis tasks
            for fs in fs_list:
                for idt, dt in enumerate(init_dates):
                    filename = fs.get_filename(dt)
                    task_vars = [var_dict for var_dict in fs.variables 
                                 if var_dict['varname_out'] in request_variables]
                    result.append(
                        STOFS3DAtlAnalysisTask(
                            filename, 
                            fs.coords,
                            task_vars, 
                            time_slices[idt], 
                            stations,
                            (version_name in ['v2.1'])
                        )
                    )
        return result

    def _get_mesh_analysis_tasks(
        self, 
        request_variables: List[str],
        stations: Iterable | pandas.DataFrame,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        forecast_type: ForecastType
    ) -> List[AnalysisTask]:
        """Return a list of MeshAnalysisTask objects for this model.

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

        Returns
        -------
        List of MeshAnalysisTask objects for this model.

        """ 
        result = []
        # Get versions
        for (version_name, version_start, version_end) in \
            self.get_versions_by_date(start_date, end_date):
            
            # Get date(s)
            if forecast_type == ForecastType.FORECAST:
                # Just a single date for forecast, but formed as a list.
                init_dates, time_slices = self.get_init_time_forecast(
                    version_start
                )
                file_hours = ['f001_012','f013_024','f025_036','f037_048',
                              'f049_060','f061_072','f073_084','f085_096']
            else:
                # A list of dates for nowcast
                init_dates, time_slices = self.get_init_times_nowcast(
                    version_start, version_end
                )
                file_hours = ['n001_012','n013_024']
        
            # Get FieldSources
            fs_list = []
            for var in request_variables:
                fs_new = self.get_field_source(version_name, var, 
                                               FileGeometry.MESH)
                for fs in fs_new:
                    if fs not in fs_list:
                        fs_list.append(fs)
            
            # Create analysis tasks
            for fs in fs_list:
                task_vars = [var_dict for var_dict in fs.variables 
                             if var_dict['varname_out'] in request_variables]
                for idt, dt in enumerate(init_dates):
                    for fh in file_hours:
                        filename = fs.get_filename(dt, {'file_hour':fh})
                        result.append(
                            MeshAnalysisTask(
                                filename, 
                                fs.coords,
                                task_vars, 
                                time_slices[idt], 
                                stations,
                                fs.file_format
                            )
                        )
        return result


        