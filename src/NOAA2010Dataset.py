import os
from typing import Optional, Dict

import numpy as np
import pandas as pd
import requests


class NOAA2010Dataset(object):
    """
    This dataset contains hourly measurements of different attributes like air temperature, dewp, and more.
    """

    MIAMI_FL: str = "miami_florida"
    FRESNO_CA: str = "fresno_california"
    OLYMPIA_WA: str = "olympia_washington"
    ROCHESTER_NY: str = "rochester_newyork"

    def __init__(self):
        self._dataset_url = "https://www.ncei.noaa.gov/orders/cdo/2209004.csv"
        self._dataset_local_extract_path = os.path.join(".", "data", "NOAA2010Dataset")
        self._dataset_path = os.path.join(self._dataset_local_extract_path, "data.csv")

        self.processed_miami_fl_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                            "processed",
                                                            "miami_fl_data.csv")
        self.processed_fresno_ca_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                             "processed",
                                                             "fresno_ca_data.csv")
        self.processed_olympia_wa_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                              "processed",
                                                              "olympia_wa_data.csv")
        self.processed_rochester_ny_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                                "processed",
                                                                "rochester_ny_data.csv")

        self.miami_fl_station_id: str = "USW00012839"
        self.fresno_ca_station_id: str = "USW00093193"
        self.olympia_wa_station_id: str = "USW00024227"
        self.rochester_ny_station_id: str = "USW00014768"

        self._all_data: Optional[pd.DataFrame] = None
        self.data: Dict = dict()
        self.processed_data: Dict = dict()

        self.dataset_exists_locally = (os.path.exists(self._dataset_path)
                                       and os.path.isfile(self._dataset_path))

    def _save_dataset_on_local_disk(self):
        dataset_content = requests.get(self._dataset_url).content
        with open(self._dataset_path, mode="wb") as dataset_file:
            dataset_file.write(dataset_content)

        self.dataset_exists_locally = True

    def _load_all_data(self):
        if not self.dataset_exists_locally:
            self._save_dataset_on_local_disk()

        self._all_data = pd.read_csv(self._dataset_path,
                                     sep=",",
                                     header=0,
                                     error_bad_lines=False,
                                     warn_bad_lines=True,
                                     names=["station_id",
                                       "station_name",
                                       "latitude",
                                       "longitude",
                                       "height",
                                       "timestamp",
                                       "hourly_cldh",
                                       "hourly_cldh_attributes",
                                       "hourly_dewp",
                                       "hourly_dewp_attributes",
                                       "hourly_hidx",
                                       "hourly_hidx_attributes",
                                       "hourly_htdh",
                                       "hourly_htdh_attributes",
                                       "air_temp",
                                       "air_temp_attributes",
                                       "hourly_wchl",
                                       "hourly_wchl_attributes"],
                                     dtype={"station_id": "object",
                                       "station_name": "object",
                                       "latitude": "object",
                                       "longitude": "object",
                                       "height": np.float32,
                                       "timestamp": "object",
                                       "hourly_cldh": np.float32,
                                       "hourly_cldh_attributes": "category",
                                       "hourly_dewp": np.float32,
                                       "hourly_dewp_attributes": "category",
                                       "hourly_hidx": np.float32,
                                       "hourly_hidx_attributes": "category",
                                       "hourly_htdh": np.float32,
                                       "hourly_htdh_attributes": "category",
                                       "air_temp": np.float32,
                                       "air_temp_attributes": "category",
                                       "hourly_wchl": np.float32,
                                       "hourly_wchl_attributes": "category"},
                                     usecols=["station_id", "station_name", "timestamp", "air_temp"])

        self._all_data["timestamp"] = (pd.to_datetime(self._all_data["timestamp"],
                                                      errors="raise",
                                                      dayfirst=False,
                                                      yearfirst=False,
                                                      utc=None,
                                                      format="%m-%dT%H:%M:%S")
                                       .apply(lambda date: date.replace(year=2010)))

        self._all_data = self._all_data.set_index("timestamp")

        self.data = {self.MIAMI_FL: self._all_data[self._all_data.station_id == self.miami_fl_station_id],
                     self.FRESNO_CA: self._all_data[self._all_data.station_id == self.fresno_ca_station_id],
                     self.OLYMPIA_WA: self._all_data[self._all_data.station_id == self.olympia_wa_station_id],
                     self.ROCHESTER_NY: self._all_data[self._all_data.station_id == self.rochester_ny_station_id]}

    def load_data(self, reload: bool = False) -> Dict:
        if reload or len(self.data) == 0:
            self._load_all_data()

        return self.data.copy()

    def _load_all_processed_data(self):
        keys_with_paths = [(self.MIAMI_FL, self.processed_miami_fl_dataset_path),
                           (self.FRESNO_CA, self.processed_fresno_ca_dataset_path),
                           (self.OLYMPIA_WA, self.processed_olympia_wa_dataset_path),
                           (self.ROCHESTER_NY, self.processed_rochester_ny_dataset_path)]

        for k, file_path in keys_with_paths:
            self.processed_data[k] = pd.read_csv(file_path,
                                                 index_col=0,
                                                 header=0,
                                                 parse_dates=True,
                                                 infer_datetime_format=True,
                                                 dtype={"air_temp": np.float32,
                                                        "dayofyear": np.int32,
                                                        "hourofyear": np.int32,
                                                        "air_temp_fit": np.float32})

    def load_processed_data(self, reload: bool = False):
        if reload or len(self.processed_data) == 0:
            self._load_all_processed_data()

        return self.processed_data.copy()
