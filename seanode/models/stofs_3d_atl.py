"""
"""


# External libraries
from typing import List, Tuple
import datetime
# This package
from seanode.analysis_task import AnalysisTask, STOFS3DAtlAnalysisTask
from seanode.models.model_task_creator import ModelTaskCreator
from seanode.request_options import FileGeometry, ForecastType
from seanode.field_source import FieldSource


class STOFS3DAtlTaskCreator(ModelTaskCreator):
    """
    """

    bucket_name = 'noaa-nos-stofs3d-pds/STOFS-3D-Atl'
    dir_prefix = 'stofs_3d_atl'
    file_prefix = 'stofs_3d_atl'
    geometry_mapper = {'points':'points', 'mesh':'fields'}
    cycles = (12,)
    nowcast_period = 24
    data_catalog = {
        'v2.1':{
            'first_run': datetime.datetime(2024, 5, 14, 12, 0),
            'last_run': None,
            'field_sources':[
                FieldSource('cwl', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'cwl', 'varname_file':'zeta', 'datum':'NAVD88'}]),
                FieldSource('cwl.temp.salt.vel', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'temperature', 'varname_file':'temperature', 'datum':None},
                             {'varname_out':'salinity', 'varname_file':'salinity', 'datum':None},
                             {'varname_out':'u_vel', 'varname_file':'u', 'datum':None},
                             {'varname_out':'v_vel', 'varname_file':'v', 'datum':None}])
            ]
        },
        'v1.1':{
            'first_run':datetime.datetime(2023, 1, 12, 12, 0),
            'last_run': datetime.datetime(2024, 5, 13, 12, 0),
            'field_sources':[
                FieldSource('cwl', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'cwl', 'varname_file':'zeta', 'datum':'NAVD88'}]),
                FieldSource('cwl.temp.salt.vel', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'temperature', 'varname_file':'temperature', 'datum':None},
                             {'varname_out':'salinity', 'varname_file':'salinity', 'datum':None},
                             {'varname_out':'u_vel', 'varname_file':'u', 'datum':None},
                             {'varname_out':'v_vel', 'varname_file':'v', 'datum':None}])
            ]
        }
    }

    def __init__(self):
        super().__init__(self.cycles, self.nowcast_period, self.data_catalog)
    
    def get_analysis_tasks(
        self, 
        request_variables,
        stations,
        start_date,
        end_date,
        forecast_type,
        geometry
    ) -> List[AnalysisTask]:
        """
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
                    filename = self.get_filename(dt, fs.file_geometry, 
                                                 fs.var_group, fs.file_format)
                    task_vars = [var_dict for var_dict in fs.variables 
                                 if var_dict['varname_out'] in request_variables]
                    result.append(
                        STOFS3DAtlAnalysisTask(
                            filename, 
                            task_vars, 
                            time_slices[idt], 
                            stations,
                            (version_name in ['v2.1'])
                        )
                    )
        return result
    
    def get_filename(
            self,
            init_datetime: datetime.datetime,
            geometry: FileGeometry,
            var_group: str,
            file_format: str
    ) -> str:
        """Get filepath for forecast initialized at init_datetime."""
        geom_name = self.geometry_mapper[geometry.value]
        yyyymmdd = init_datetime.strftime('%Y%m%d')
        hh = init_datetime.strftime('%H')
        filepath = f'{self.bucket_name}/{self.dir_prefix}.{yyyymmdd}/{self.file_prefix}.t{hh}z.{geom_name}.{var_group}.{file_format}'
        return filepath


        