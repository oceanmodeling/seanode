"""
"""


from data_store_factory import get_data_store
from model_factory import get_model
import pandas as pd


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
        print(f'Running {len(tasks)} AnalysisTasks for station data request.')
        df_list = []
        for t in tasks:
            df_list.append(t.run(self.data_store))
        df_out = pd.concat(df_list)
        return df_out

    