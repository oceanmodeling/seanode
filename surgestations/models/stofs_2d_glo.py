"""
"""


# External libraries
from typing import List, Tuple
import datetime
# This package
from surgestations.analysis_task import AnalysisTask
from surgestations.models.model_task_creator import ModelTaskCreator
from surgestations.request_options import FileGeometry, ForecastType
from surgestations.field_source import FieldSource


class STOFS2DGloTaskCreator(ModelTaskCreator):
    """
    """

    bucket_name = 'noaa-gestofs-pds'
    dir_prefix = 'stofs_2d_glo'
    file_prefix = 'stofs_2d_glo'
    geometry_mapper = {'points':'points', 'mesh':'fields'}
    cycles = (0, 6, 12, 18)
    nowcast_period = 6
    data_catalog = {
        'v2.1':{
            'first_run': datetime.datetime(2024, 5, 14, 12, 0),
            'last_run': None,
            'field_sources':[
                FieldSource('cwl', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'cwl_bias_corrected', 'varname_file':'zeta', 'datum':'LMSL'}]),
                FieldSource('cwl.noanomaly', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'cwl_raw', 'varname_file':'zeta', 'datum':'LMSL'}]),
                FieldSource('htp', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'htp', 'varname_file':'zeta', 'datum':'LMSL'}]),
                FieldSource('swl', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'swl', 'varname_file':'zeta', 'datum':'LMSL'}]),
                FieldSource('cwl.vel', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'u_vel', 'varname_file':'u-vel', 'datum':None},
                             {'varname_out':'v_vel', 'varname_file':'v-vel', 'datum':None}])
            ]
        },
        'v2.0':{
            'first_run':datetime.datetime(2020, 12, 30, 0, 0),
            'last_run': datetime.datetime(2024, 5, 14, 6, 0),
            'field_sources':[
                FieldSource('cwl', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'cwl_bias_corrected', 'varname_file':'zeta', 'datum':'MSL'}]),
                FieldSource('cwl.noanomaly', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'cwl_raw', 'varname_file':'zeta', 'datum':'MSL'}]),
                FieldSource('htp', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'htp', 'varname_file':'zeta', 'datum':'MSL'}]),
                FieldSource('swl', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'swl', 'varname_file':'zeta', 'datum':'MSL'}]),
                FieldSource('cwl.vel', 'nc', FileGeometry.POINTS,
                            [{'varname_out':'u_vel', 'varname_file':'u-vel', 'datum':None},
                             {'varname_out':'v_vel', 'varname_file':'v-vel', 'datum':None}])
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
                # Just a single date for forecast, but make it into a list.
                init_dates = [self.get_init_time_containing(version_start)]
                time_slices = [None]
            else:
                # A list of dates for nowcast
                init_dates = self.get_init_time_range(version_start, version_end)
                time_slices = [
                    (idt - datetime.timedelta(hours=self.nowcast_period), idt) 
                    for idt in init_dates
                ]
        
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
                        AnalysisTask(filename, 
                                     task_vars, 
                                     time_slices[idt], 
                                     stations)
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


        