import os
from typing import Optional

import numpy as np
import pandas as pd
import requests


class NOAA2010Dataset(object):
    def __init__(self):
        self._dataset_url = "https://www.ncei.noaa.gov/orders/cdo/2209004.csv"
        self._dataset_local_extract_path = os.path.join(".", "data", "NOAA2010Dataset")
        self._dataset_path = os.path.join(self._dataset_local_extract_path, "data.csv")

        self.data: Optional[pd.DataFrame] = None

        self.dataset_exists_locally = (os.path.exists(self._dataset_path)
                                       and os.path.isfile(self._dataset_path))

    def _save_dataset_on_local_disk(self):
        dataset_content = requests.get(self._dataset_url).content
        with open(self._dataset_path, mode="wb") as dataset_file:
            dataset_file.write(dataset_content)

        self.dataset_exists_locally = True

    def load_all_data(self):
        if not self.dataset_exists_locally:
            self._save_dataset_on_local_disk()

        self.data = pd.read_csv(self._dataset_path,
                                sep=",",
                                header=0,
                                error_bad_lines=False,
                                warn_bad_lines=True,
                                names=["station_id",
                                       "station_name",
                                       "latitude",
                                       "longitude",
                                       "height",
                                       "date",
                                       "hourly_cldh",
                                       "hourly_cldh_attributes",
                                       "hourly_dewp",
                                       "hourly_dewp_attributes",
                                       "hourly_hidx",
                                       "hourly_hidx_attributes",
                                       "hourly_htdh",
                                       "hourly_htdh_attributes",
                                       "hourly_temperature",
                                       "hourly_temperature_attributes",
                                       "hourly_wchl",
                                       "hourly_wchl_attributes"],
                                dtype={"station_id": "object",
                                       "station_name": "object",
                                       "latitude": "object",
                                       "longitude": "object",
                                       "height": np.float32,
                                       "date": "object",
                                       "hourly_cldh": np.float32,
                                       "hourly_cldh_attributes": "category",
                                       "hourly_dewp": np.float32,
                                       "hourly_dewp_attributes": "category",
                                       "hourly_hidx": np.float32,
                                       "hourly_hidx_attributes": "category",
                                       "hourly_htdh": np.float32,
                                       "hourly_htdh_attributes": "category",
                                       "hourly_temperature": np.float32,
                                       "hourly_temperature_attributes": "category",
                                       "hourly_wchl": np.float32,
                                       "hourly_wchl_attributes": "category"})

        self.data["date"] = (pd.to_datetime(self.data["date"],
                                            errors="raise",
                                            dayfirst=False,
                                            yearfirst=False,
                                            utc=None,
                                            format="%m-%dT%H:%M:%S")
                               .apply(lambda date: date.replace(year=2010)))
