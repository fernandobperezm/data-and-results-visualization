import os
from typing import Dict

import numpy as np
import pandas as pd


class InsPireDataset(object):
    """
    This dataset contains hourly measurements of different attributes like air temperature, dewp, and more.
    """

    LONDON_UK: str = "london_uk"
    MADRID_SPA: str = "madrid_spain"
    ROME_IT: str = "rome_italy"
    STUTTGART_GER: str = "stuttgart_germany"

    def __init__(self):
        self._dataset_local_extract_path = os.path.join(".", "data", "insPire")

        self._london_dataset_path = os.path.join(self._dataset_local_extract_path, "London.xls")
        self._madrid_dataset_path = os.path.join(self._dataset_local_extract_path, "Madrid.xls")
        self._rome_dataset_path = os.path.join(self._dataset_local_extract_path, "Rome.xls")
        self._stuttgart_dataset_path = os.path.join(self._dataset_local_extract_path, "Stuttgart.xls")

        self.processed_london_uk_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                             "processed",
                                                             "london_uk_data.csv")
        self.processed_madrid_spa_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                              "processed",
                                                              "madrid_spa_data.csv")
        self.processed_rome_it_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                           "processed",
                                                           "rome_it_data.csv")
        self.processed_stuttgart_ger_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                                 "processed",
                                                                 "stuttgart_ger_data.csv")

        self.data: Dict = dict()
        self.processed_data: Dict = dict()

    def _load_all_data(self):
        keys_with_paths = [(self.LONDON_UK, self._london_dataset_path),
                           (self.MADRID_SPA, self._madrid_dataset_path),
                           (self.ROME_IT, self._rome_dataset_path),
                           (self.STUTTGART_GER, self._stuttgart_dataset_path)]

        for k, file_path in keys_with_paths:
            data = pd.read_excel(file_path,
                                 header=0,
                                 names=["hourofyear", "air_temp"],
                                 usecols=["hourofyear", "air_temp"],
                                 dtype={"hourofyear": np.int32,
                                        "air_temp": np.float32})

            data["timestamp"] = pd.date_range("2017-01-01", freq="H", periods=data.shape[0])
            data = data.set_index("timestamp")

            self.data[k] = data

    def load_data(self, reload: bool = False) -> Dict:
        if reload or len(self.data) == 0:
            self._load_all_data()

        return self.data.copy()

    def _load_all_processed_data(self):
        keys_with_paths = [(self.LONDON_UK, self.processed_london_uk_dataset_path),
                           (self.MADRID_SPA, self.processed_madrid_spa_dataset_path),
                           (self.ROME_IT, self.processed_rome_it_dataset_path),
                           (self.STUTTGART_GER, self.processed_stuttgart_ger_dataset_path)]

        for k, file_path in keys_with_paths:
            self.processed_data[k] = pd.read_csv(file_path,
                                                 index_col=0,
                                                 header=0,
                                                 parse_dates=True,
                                                 infer_datetime_format=True,
                                                 dtype={"air_temp": np.float32,
                                                        "dayofyear": np.int32,
                                                        "hourofyear": np.int32,
                                                        "air_temp_fit": np.float32}
                                                 )

    def load_processed_data(self, reload: bool = False):
        if reload or len(self.processed_data) == 0:
            self._load_all_processed_data()

        return self.processed_data.copy()
