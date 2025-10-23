"""Define the request class with methods to gather and format model data.

Classes
-------
SurgeModelRequest

"""


import pandas
import numpy
from typing import List, Iterable
import datetime
from seanode.data_store_factory import get_data_store
from seanode.model_factory import get_model
from seanode import request_options
import logging


logger = logging.getLogger(__name__)


class SurgeModelRequest:
    """Define a model data request.

    Usually, this will be created by a call to a function in api.py
    Best not to create this request object directly. 

    Attributes
    ----------
    model_name
        The name of the model for this request.
    model
        A model object (ModelTaskCreator class).
    data_store_name
        The name of the data store.
    data_store
        A DataStore object.
    variables
        List of data variables to retrieve.
    stations
        A list or other object containing information about the 
        locations at which to retrieve data.
    start_date
        Start of period from which to retrieve data.
    end_date
        End of period from which to retrieve data. Ignored for
        forecast of type "forecast".
    forecast_type
        Whether to get nowcast or forecast data.
    geometry
        Type of files from which to retrieve data.
    output_datum
        Datum relative to which water level data will be returned.
    
    Methods
    -------
    run()
        Run the request, submitting a series of analysis tasks and 
        gathering all the outputs. 
    
    """

    def __init__(
        self,
        model: request_options.ModelOptions,
        variables: List[str],
        stations: Iterable | pandas.DataFrame,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        forecast_type: request_options.ForecastType,
        geometry: request_options.FileGeometry,
        output_datum: str,
        data_store: request_options.DataStoreOptions
    ) -> None:
        """Initialize a model request object.

        Parameters
        ----------
        model
            The name of the model for this request
        variables
            List of data variables to retrieve
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
        output_datum
            Datum relative to which water level data will be returned.
        data_store
            The name of the data store.
        
        """
        self.model_name = model
        self.variables = variables
        self.stations = stations
        self.start_date = start_date
        self.end_date = end_date
        self.forecast_type = forecast_type
        self.geometry = geometry
        self.output_datum = output_datum
        self.data_store_name = data_store
        self.model = get_model(self.model_name)
        self.data_store = get_data_store(self.data_store_name)

    def run(self):
        """Run a model data request.
        
        Gets a set of tasks, one per output file, runs them all,
        then gathers and combines the output.
        
        """
        tasks = self.model.get_analysis_tasks(
            self.variables,
            self.stations,
            self.start_date,
            self.end_date,
            self.forecast_type,
            self.geometry
        )
        logger.info(f'Running {len(tasks)} AnalysisTasks for station data request.')
        df_list = []
        for t in tasks:
            try:
                df_list.append(t.run(self.data_store, self.output_datum))
            except:
                logger.warning(f'Cannot complete analysis task on file {t.filename}.')
        if df_list:
            df_out = self._concat_and_update(df_list)
        else:
            logger.warning('No data frames returned from AnalysisTasks.')
            df_out = pandas.DataFrame()
        return df_out

    def _concat_and_update(
        self, 
        df_list: List[pandas.DataFrame]
    ) -> pandas.DataFrame:
        """Combine a list of data frames into a single data frame.

        This function adds new columns and indices as needed, and populates
        any NaN data where possible.

        Parameters
        ----------
        df_list
            List of data frames with (station, time) multiindices.
            Usually, each will be the output of an AnalysisTask.run()
            operation.

        Returns
        -------
        pandas.DataFrame
            Containing combined data of all data frames in the input. 
            
        """
        result = df_list[0]

        if len(df_list) > 1:
            for df in df_list[1:]:
                
                not_indexed = df.index[[ind not in result.index for ind in df.index]]
                new_cols = [col for col in df.columns if col not in result.columns]
                    
                # Concatenate new indices or new columns if needed.
                if any(not_indexed):
                    result = result.reindex(result.index.append(not_indexed))
                if any(new_cols):
                    result[new_cols] = numpy.nan

                # Overwrite NaNs in output dataframe.
                result.update(df, overwrite=False)
                
        return result

    