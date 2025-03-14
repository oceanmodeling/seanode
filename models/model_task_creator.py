"""
"""


# External libraries
from typing import List, Tuple
import datetime
import sys
# This package
from analysis_task import AnalysisTask
from request_options import FileGeometry
from field_source import FieldSource


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

    def get_init_time_containing(
        self,
        start_date: datetime.datetime
    ) -> datetime.datetime:
        """Get the initialization datetime for latest forecast containing start_date.
    
        Note that the returned initialization datetime is equal or earlier than
        start_date, so the start_date occurs in the forecast period, not the
        now cast period of the model run. 
    
        Parameters
        ----------
        start_date
            The datetime which the forecast must contain.
    
        Returns
        -------
        datetime
            The latest/last forecast initialization time that 
            contains the specified start time. 
    
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
            return max(cycle_dates) - datetime.timedelta(days=1)
        else:
            return max([dt for (dt, b) in zip(cycle_dates, contains_start_date) if b])

    def get_init_time_range(
            self,
            start_date: datetime.datetime,
            end_date: datetime.datetime
    ) -> List[datetime.datetime]:
        """Get list of initialization times covering a given period.
    
        This method errs on the conservative side, to try and ensure that the
        nowcasts of the returned initialization times cover the entire
        period.
    
        Parameters
        ----------
        start_date
            The first day for which initialization times are retrieved.
        end_date
            The last day for which initialization times are retrieved,
            though note that one more cycle beyond this day is retrieved.
        cycles
            The times (hour in range 0 to 23) of daily forecast cycles.
    
        Returns
        -------
        List of datetimes
            One per cycle per day between start_date and end_date 
            (inclusive) plus the first cycle of an extra day.
    
        """
        result = []
        dd = start_date.date()
        
        while dd <= end_date.date():
            for cc in self.cycles:
                result.append(
                    datetime.datetime(dd.year,
                                      dd.month,
                                      dd.day,
                                      cc)
                )
            dd = dd + datetime.timedelta(days=1)
        
        # Add one more init time from the next day, to make
        # sure the nowcasts cover the entire period.
        extra_day = end_date + datetime.timedelta(days=1)
        result.append(datetime.datetime(extra_day.year,
                                        extra_day.month,
                                        extra_day.day,
                                        min(self.cycles)))
    
        return result