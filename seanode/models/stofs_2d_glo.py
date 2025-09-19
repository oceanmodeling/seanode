"""STOFS-2D-Global model and task creator.

Classes
-------
STOFS2DGloTaskCreator

"""


# External libraries
from typing import List, Tuple, Iterable
import datetime
import pandas
# This package
from seanode.analysis_task import AnalysisTask
from seanode.analysis_task_mesh import MeshAnalysisTask
from seanode.models.model_task_creator import ModelTaskCreator
from seanode.request_options import FileGeometry, ForecastType
from seanode.field_source import FieldSource


class STOFS2DGloTaskCreator(ModelTaskCreator):
    """STOFS-2D-Global model and task creator.

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
    
    """

    cycles = (0, 6, 12, 18)
    nowcast_period = 6
    data_catalog = {
        'v2.1':{
            'first_run': datetime.datetime(2024, 5, 14, 12, 0),
            'last_run': None,
            'field_sources':[
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.points.cwl.nc',
                            [{'varname_out':'cwl_bias_corrected', 'varname_file':'zeta', 'datum':'LMSL'}],
                            {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                            FileGeometry.POINTS, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.points.cwl.noanomaly.nc',
                            [{'varname_out':'cwl_raw', 'varname_file':'zeta', 'datum':'LMSL'}],
                            {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                            FileGeometry.POINTS, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.points.htp.nc',
                            [{'varname_out':'htp', 'varname_file':'zeta', 'datum':'LMSL'}],
                            {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                            FileGeometry.POINTS, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.points.swl.nc',
                            [{'varname_out':'swl', 'varname_file':'zeta', 'datum':None}],
                            {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                            FileGeometry.POINTS, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.points.cwl.vel.nc',
                            [{'varname_out':'u_vel', 'varname_file':'u-vel', 'datum':None},
                             {'varname_out':'v_vel', 'varname_file':'v-vel', 'datum':None}],
                            {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                            FileGeometry.POINTS, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.fields.cwl.nc',
                            [{'varname_out':'cwl_raw', 'varname_file':'zeta', 'datum':'LMSL'}],
                            {'latitude':'y', 'longitude':'x', 'time':'time'},
                            FileGeometry.MESH, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.fields.htp.nc',
                            [{'varname_out':'htp', 'varname_file':'zeta', 'datum':'LMSL'}],
                            {'latitude':'y', 'longitude':'x', 'time':'time'},
                            FileGeometry.MESH, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.fields.swl.nc',
                            [{'varname_out':'swl', 'varname_file':'zeta', 'datum':None}],
                            {'latitude':'y', 'longitude':'x', 'time':'time'},
                            FileGeometry.MESH, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.fields.cwl.vel.nc',
                            [{'varname_out':'u_vel', 'varname_file':'u-vel', 'datum':None},
                             {'varname_out':'v_vel', 'varname_file':'v-vel', 'datum':None}],
                            {'latitude':'y', 'longitude':'x', 'time':'time'},
                            FileGeometry.MESH, 'nc')
            ]
        },
        'v2.0':{
            'first_run':datetime.datetime(2020, 12, 30, 0, 0),
            'last_run': datetime.datetime(2024, 5, 14, 6, 0),
            'field_sources':[
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.points.cwl.nc',
                            [{'varname_out':'cwl_raw', 'varname_file':'zeta', 'datum':'LMSL'}],
                            {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                            FileGeometry.POINTS, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.points.htp.nc',
                            [{'varname_out':'htp', 'varname_file':'zeta', 'datum':'LMSL'}],
                            {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                            FileGeometry.POINTS, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.points.swl.nc',
                            [{'varname_out':'swl', 'varname_file':'zeta', 'datum':'LMSL'}],
                            {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                            FileGeometry.POINTS, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.points.cwl.vel.nc',
                            [{'varname_out':'u_vel', 'varname_file':'u-vel', 'datum':None},
                             {'varname_out':'v_vel', 'varname_file':'v-vel', 'datum':None}],
                            {'latitude':'y', 'longitude':'x', 'time':'time', 'station_name':'station_name'},
                            FileGeometry.POINTS, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.fields.cwl.nc',
                            [{'varname_out':'cwl_raw', 'varname_file':'zeta', 'datum':'LMSL'}],
                            {'latitude':'y', 'longitude':'x', 'time':'time'},
                            FileGeometry.MESH, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.fields.htp.nc',
                            [{'varname_out':'htp', 'varname_file':'zeta', 'datum':'LMSL'}],
                            {'latitude':'y', 'longitude':'x', 'time':'time'},
                            FileGeometry.MESH, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.fields.swl.nc',
                            [{'varname_out':'swl', 'varname_file':'zeta', 'datum':None}],
                            {'latitude':'y', 'longitude':'x', 'time':'time'},
                            FileGeometry.MESH, 'nc'),
                FieldSource('noaa-gestofs-pds/stofs_2d_glo.{yyyymmdd}/stofs_2d_glo.t{hh}z.fields.cwl.vel.nc',
                            [{'varname_out':'u_vel', 'varname_file':'u-vel', 'datum':None},
                             {'varname_out':'v_vel', 'varname_file':'v-vel', 'datum':None}],
                            {'latitude':'y', 'longitude':'x', 'time':'time'},
                            FileGeometry.MESH, 'nc')
            ]
        }
    }

    def __init__(self):
        """Initialize STOFS2DGloTaskCreator.

        Note that this simply initializes the parent class with the 
        STOFS2DGloTaskCreator class variables.

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
                fs_new = self.get_field_source(version_name, var, geometry)
                for fs in fs_new:
                    if fs not in fs_list:
                        fs_list.append(fs)
            
            # Create analysis tasks
            for fs in fs_list:
                for idt, dt in enumerate(init_dates):
                    filename = fs.get_filename(dt)
                    task_vars = [var_dict for var_dict in fs.variables 
                                 if var_dict['varname_out'] in request_variables]
                    if geometry == FileGeometry.MESH:
                        result.append(
                            MeshAnalysisTask(filename, 
                                             fs.coords,
                                             task_vars, 
                                             time_slices[idt], 
                                             stations,
                                             fs.file_format)
                        )
                    else:
                        result.append(
                            AnalysisTask(filename, 
                                         fs.coords,
                                         task_vars, 
                                         time_slices[idt], 
                                         stations)
                        )
        return result


        