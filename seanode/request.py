"""
"""


import pandas as pd
import numpy as np
from seanode.data_store_factory import get_data_store
from seanode.model_factory import get_model
import logging


logger = logging.getLogger(__name__)


class SurgeModelRequest:
    """
    """

    def __init__(
        self,
        model,
        variables,
        stations,
        start_date,
        end_date,
        forecast_type,
        geometry,
        output_datum,
        data_store
    ) -> None:
        """
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
        """
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
            df_list.append(t.run(self.data_store, self.output_datum))
        df_out = self._concat_and_update(df_list)
        return df_out

    def _concat_and_update(self, df_list):
        """
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
                    result[new_cols] = np.nan

                # Overwrite NaNs in output dataframe.
                result.update(df, overwrite=False)
                
        return result

    