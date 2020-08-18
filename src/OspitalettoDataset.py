import os
import numpy as np
import pandas as pd

from typing import Optional


class OspitalettoDataset(object):
    OSPITALETTO: str = "ospitaletto"

    def __init__(self):
        self._dataset_local_extract_path = os.path.join(".", "data", "Ospitaletto")
        self._dataset_path = os.path.join(self._dataset_local_extract_path, "data.csv")

        self.processed_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                   "processed",
                                                   "data.csv")

        self.data: Optional[pd.DataFrame] = None
        self.processed_data: Optional[pd.DataFrame] = None

    def load_all_data(self):
        self.data = pd.read_csv(self._dataset_path,
                                names=["fist", "timestamp", "air_temp"],
                                usecols=["timestamp", "air_temp"],
                                index_col=0,
                                header=0,
                                parse_dates=True,
                                infer_datetime_format=True)
        # Remove invalid values
        # Another option is to set these values to 15 using: df.loc[df['Temp'] == -999, 'Temp'] = 15
        # Or using the mean: df.loc[df['Temp'] == -999, 'Temp'] = df['Temp'].mean()
        self.data = self.data[(self.data.air_temp != 999.0)
                              & (self.data.air_temp != -999.0)]

        return {self.OSPITALETTO: self.data.copy()}

    def load_hourly_data(self):
        if self.data is None:
            self.load_all_data()

        return self.data['air_temp'].resample('H').mean().to_frame()

    def load_processed_data(self, reload: bool = False):
        if reload or self.processed_data is None:
            self.processed_data = pd.read_csv(self.processed_dataset_path,
                                              index_col=0,
                                              header=0,
                                              parse_dates=True,
                                              infer_datetime_format=True,
                                              dtype={"air_temp": np.float32,
                                                     "dayofyear": np.int32,
                                                     "hourofyear": np.int32,
                                                     "air_temp_fit": np.float32})

        return {self.OSPITALETTO: self.processed_data.copy()}


