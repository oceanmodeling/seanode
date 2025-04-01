"""
"""


# External libraries
from typing import List, Tuple
import datetime
import sys
# This package
from seanode.analysis_task import AnalysisTask
from seanode.request_options import FileGeometry
from seanode.field_source import FieldSource


class ModelTaskCreator:
    """
    """

    def __init__(
        self,
        cycles,
        nowcast_period,
        data_catalog
    ) -> None:
        """
        """
        self.cycles = cycles
        self.nowcast_period = nowcast_period
        self.data_catalog = data_catalog
    
    def get_analysis_tasks(self) -> List[AnalysisTask]:
        """
        """
        print('get_analysis_tasks() not implemented for generic model base class.')
        return []

    def get_versions_by_date(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime
    ) -> List[Tuple[str, datetime.datetime, datetime.datetime]]:
        """
        """
        result = []
        for ver in self.data_catalog.keys():
            fr = self.data_catalog[ver]['first_run']
            lr = self.data_catalog[ver]['last_run']
            if lr: 
                if (start_time <= lr) & (end_time >= fr): 
                    result.append(
                        (ver, 
                         max(start_time, fr), 
                         min(end_time, lr))
                    ) 
            else: 
                if end_time >= fr: 
                    result.append(
                        (ver,
                         max(start_time, fr),
                         end_time)
                    ) 
        return result 
        
    def get_field_source(
        self, 
        version: str,
        var: str, 
        geometry: FileGeometry
    ) -> List[FieldSource]:
        """Returns list of field sources for given variable and geometry.

        """
        result = []
        for fs in self.data_catalog[version]['field_sources']:
            if fs.file_geometry == geometry:
                if var in fs.get_vars():
                    result.append(fs)

        # Check if exactly 1 DataSource in result
        if len(result) > 1:
            sys.exit(f'warning: more than one FieldSource available for variable {var} in {geometry} files.')
        elif len(result) == 0:
            sys.exit(f'warning: no FieldSource available for variable {var} in {geometry} files.')
        else:
            return result
            
    def get_init_time_forecast(
        self,
        start_date: datetime.datetime
    ) -> Tuple[List[datetime.datetime], 
               List[Tuple[datetime.datetime, datetime.datetime]]]:
        """Get the initialization datetime for latest forecast containing start_date.
    
        Note that the returned initialization datetime is equal or earlier than
        start_date, so the start_date occurs in the forecast period, not the
        nowcast period of the model run. The returned time slice is formed
        such that the nowcast period is excluded.
    
        Parameters
        ----------
        start_date
            The datetime which the forecast must contain.
    
        Returns
        -------
        datetime (as a single value in list)
            The latest/last forecast initialization time that 
            contains the specified start time. 
        time slice (as a single object in list)
            The period to retrieve for the initialization time.
    
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
        return [init_date], [(init_date, None)]

    def get_init_times_nowcast(
            self,
            start_date: datetime.datetime,
            end_date: datetime.datetime
    ) -> Tuple[List[datetime.datetime], 
               List[Tuple[datetime.datetime, datetime.datetime]]]:
        """Get list of initialization times whose nowcast covers a given period.
    
        This method assumes that files contain nowcast timesteps in the interval
            (init_time - nowcast_period, init_time]
        E.g., 12:06 - 18:00 for a points file from model run initialized at 18:00,
        or 13:00 - 18:00 for a fields file from model run initialized at 18:00.
    
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
        List of time slices
            The period of nowcast to retrieve for each initialization.
            One for each value in the list of initialization datetimes.
    
        """
        result = []
        slice_result = []
        
        dd = start_date.date()
        while dd <= end_date.date() + datetime.timedelta(days=1):
            for cc in self.cycles:
                nowcast_start = (
                    datetime.datetime(dd.year, dd.month, dd.day, cc)
                    - datetime.timedelta(hours=self.nowcast_period)
                )
                nowcast_end = (
                    datetime.datetime(dd.year, dd.month, dd.day, cc)
                )
                if ((nowcast_end >= start_date) & (nowcast_start < end_date)):
                    result.append(
                        datetime.datetime(dd.year,
                                          dd.month,
                                          dd.day,
                                          cc)
                    )
                    slice_result.append(
                        (max(nowcast_start, start_date),
                         min(nowcast_end, end_date))
                    )
            dd = dd + datetime.timedelta(days=1)
        return result, slice_result