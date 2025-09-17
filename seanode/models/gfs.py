"""GFS model and task creator.

Classes
-------
GFSTaskCreator

"""


from typing import List, Tuple, Iterable
import pandas
import datetime
import dask
import itertools
import math
from seanode.analysis_task import AnalysisTask
from seanode.analysis_task_grid import GridAnalysisTask
from seanode.models.model_task_creator import ModelTaskCreator
from seanode.request_options import FileGeometry, ForecastType
from seanode.field_source import FieldSource
from seanode.kerchunker import kerchunk_grib


class GFSTaskCreator(ModelTaskCreator):
    """GFS model and task creator.

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

    bucket_name = 'noaa-gfs-bdp-pds'
    dir_prefix = 'gfs'
    file_prefix = 'gfs'
    cycles = (0, 6, 12, 18)
    nowcast_period = 6
    data_catalog = {
        'v1':{
            'first_run': datetime.datetime(2021, 3, 22, 12, 0),
            'last_run': None,
            'field_sources':[
                FieldSource('sfluxgrb', 'kerchunk', FileGeometry.GRID,
                            [{'varname_out':'ps', 'varname_file':'sp', 'datum':None},
                             {'varname_out':'u10', 'varname_file':'u10', 'datum':None},
                             {'varname_out':'v10', 'varname_file':'v10', 'datum':None}],
                            {'time':'valid_time', 'init_time':'time', 
                             'latitude':'latitude','longitude':'longitude'})
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
            
            # Get initialization datetime(s) and list of lead times for each.
            if forecast_type == ForecastType.FORECAST:
                # Just a single date for forecast, but formed as a list.
                init_dates, lead_times = self.get_init_time_forecast(
                    version_start
                )
            else:
                # A list of dates for nowcast
                init_dates, lead_times = self.get_init_times_nowcast(
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
                ref_files = []
                for idt, dt in enumerate(init_dates):
                    for lt in lead_times[idt]:
                        # Get name of the grib file.
                        # Note we hard-code the "grib2" suffix so it
                        # doesn't look for "kerchunk" file suffixs.
                        filename = self.get_filename(dt, lt, 
                                                     fs.var_group, 
                                                     'grib2')
                        # Get reference files for this grib file.
                        ltrefs = dask.delayed(kerchunk_grib)(filename)
                        # Add to overall list for this task.
                        ref_files.append(ltrefs)
                ref_files = dask.compute(*ref_files)
                task_vars = [var_dict for var_dict in fs.variables 
                             if var_dict['varname_out'] in request_variables]
                result.append(
                    GridAnalysisTask(list(itertools.chain(*ref_files)), 
                                     fs.coords,
                                     task_vars,
                                     None,
                                     stations,
                                     fs.file_format)
                )
        return result
    
    def get_filename(
            self,
            init_datetime: datetime.datetime,
            forecast_lead: int,
            var_group: str,
            file_format: str
    ) -> str:
        """Get filepath for forecast initialized at a specific time.

        Parameters
        ----------
        init_datetime
            The model run initialization time.
        forecast_lead
            Lead time (in whole hours) of forecast in this file.
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
        filepath = f'{self.bucket_name}/{self.dir_prefix}.{yyyymmdd}/{hh}/atmos/{self.file_prefix}.t{hh}z.{var_group}f{forecast_lead:03d}.{file_format}'
        return filepath

    def get_init_time_forecast(
        self,
        start_date: datetime.datetime
    ) -> Tuple[List[datetime.datetime], 
               List[Tuple]]:
        """Get the initialization datetime for latest forecast containing start_date.
    
        ...and also the list of lead times that correspond to files.
        
        Note that the returned initialization datetime is equal or earlier than
        start_date, so the start_date occurs in the forecast period.
    
        Parameters
        ----------
        start_date
            The datetime which the forecast must contain.
    
        Returns
        -------
        datetime (as a single value in list)
            The latest/last forecast initialization time that 
            contains the specified start time. 
        tuple of lead times (as a single value in a list)
    
        """
        # Get cycle hours as full datetimes.
        cycle_dates = [datetime.datetime(start_date.year, 
                                         start_date.month, 
                                         start_date.day, 
                                         c) 
                       for c in self.cycles]
    
        # Work out which cycles contain the start date.
        contains_start_date = [cd <= start_date for cd in cycle_dates]
    
        # If none of today's cycle_dates contain start_date, go to the 
        # last cycle from the day before. Otherwise, get the latest cycle 
        # which does contain start_date.
        if not any(contains_start_date):
            init_date =  max(cycle_dates) - datetime.timedelta(days=1)
        else:
            init_date =  max([dt for (dt, b) 
                              in zip(cycle_dates, contains_start_date) if b])
        lead_times = tuple(range(0,120,1)) + tuple(range(120,181,3))
        return [init_date], [lead_times]

    def get_init_times_nowcast(
            self,
            start_date: datetime.datetime,
            end_date: datetime.datetime
    ) -> Tuple[List[datetime.datetime], 
               List[Tuple]]:
        """Get list of initialization times whose nowcast covers a given period.

        ...and also the list of lead times that correspond to files.
    
        This method defines the nowcast period to be the interval
            [init_time, init_time + nowcast_period)
        E.g., 12:00 - 17:00 for a file from model run initialized at 12:00.
    
        Parameters
        ----------
        start_date
            The first day for which initialization times are retrieved.
        end_date
            The last day for which initialization times are retrieved,
            though note that one more cycle beyond this day is retrieved.
    
        Returns
        -------
        List of initialization datetimes
            One per cycle to ensure the nowcast period covers the time 
            between start_date and end_date.
        List of tuples of lead times
            The forecast hours to retrieve for each initialization.
            One tuple for each value in the list of initialization datetimes.
    
        """
        result = []
        lead_result = []
        
        dd = start_date.date()
        while dd <= end_date.date() + datetime.timedelta(days=1):
            for cc in self.cycles:
                nowcast_start = (
                    datetime.datetime(dd.year, dd.month, dd.day, cc)
                )
                nowcast_end = (
                    datetime.datetime(dd.year, dd.month, dd.day, cc)
                    + datetime.timedelta(hours=self.nowcast_period)
                )
                if ((nowcast_end > start_date) & (nowcast_start <= end_date)):
                    result.append(
                        datetime.datetime(dd.year,
                                          dd.month,
                                          dd.day,
                                          cc)
                    )
                    lead_result.append(
                        tuple(
                            range(
                                6 - min(6,math.ceil((nowcast_end - start_date)/datetime.timedelta(hours=1))),
                                1 + min(5,math.ceil((end_date - nowcast_start)/datetime.timedelta(hours=1)))
                            )
                        )
                    )
            dd = dd + datetime.timedelta(days=1)
        return result, lead_result


        