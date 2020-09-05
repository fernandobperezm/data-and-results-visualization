import os
from typing import Optional, Dict

import numpy as np
import pandas as pd


class NOAA2010Dataset(object):
    """
    This dataset contains hourly measurements of different attributes like air temperature, dewp, and more.
    """

    MIAMI_FL: str = "miami_florida"
    FRESNO_CA: str = "fresno_california"
    OLYMPIA_WA: str = "olympia_washington"
    ROCHESTER_NY: str = "rochester_newyork"

    def __init__(self):
        self._dataset_local_extract_path = os.path.join(".", "data", "NOAA2010Dataset")

        self._heat_demand_path = os.path.join(self._dataset_local_extract_path, "heat_demand.xlsx")
        self._dhw_profile_path = os.path.join(self._dataset_local_extract_path, "dhw_random_profile.xlsx")

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

        self._heat_demand: Optional[pd.DataFrame] = None
        self._dhw_profile: Optional[pd.Series] = None
        self._all_data: Optional[pd.DataFrame] = None

        self.data: Dict = dict()
        self.processed_data: Dict = dict()

    def _load_all_data(self):
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
        def calculate_season(data: pd.DataFrame):
            winter_ends = "2010-03-20"
            spring_ends = "2010-06-21"
            summer_ends = "2010-09-22"
            fall_ends = "2010-12-21"

            data["season"] = "-"

            winter_mask = (data.index >= fall_ends) | (data.index < winter_ends)
            spring_mask = (data.index >= winter_ends) & (data.index < spring_ends)
            summer_mask = (data.index >= spring_ends) & (data.index < summer_ends)
            fall_mask = (data.index >= summer_ends) & (data.index < fall_ends)

            data.loc[winter_mask, "season"] = "winter"
            data.loc[spring_mask, "season"] = "spring"
            data.loc[summer_mask, "season"] = "summer"
            data.loc[fall_mask, "season"] = "fall"

            return data["season"]

        keys_with_paths = [(self.MIAMI_FL, self.processed_miami_fl_dataset_path),
                           (self.FRESNO_CA, self.processed_fresno_ca_dataset_path),
                           (self.OLYMPIA_WA, self.processed_olympia_wa_dataset_path),
                           (self.ROCHESTER_NY, self.processed_rochester_ny_dataset_path)]

        self._heat_demand = pd.read_excel(self._heat_demand_path)
        self._dhw_profile = pd.read_excel(self._dhw_profile_path, index_col="Hour")

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

            num_rows_in_processed_data = self.processed_data[k].shape[0]
            num_days = self.processed_data[k].dayofyear.max()

            # We select the current city (determined by k and matched with the city_key column)
            # Then we repeat the rows in the dataset for num_rows_in_processed_data times
            # Third, we reindex the dataframe using the self.processed_data[k] index
            # Lastly, we concat the heat demand columns into the processed data
            heat_demand_for_city = self._heat_demand[self._heat_demand["city_key"] == k]
            heat_demand_for_city = pd.concat([heat_demand_for_city] * num_rows_in_processed_data)
            heat_demand_for_city = pd.DataFrame(heat_demand_for_city.values,
                                                columns=heat_demand_for_city.columns,
                                                index=self.processed_data[k].index)

            # The DHW profile is constant through the year but changes during the day (however, it keeps the same
            # values for the same hour in different days). The NOAA2010 dataset does not contain the first hour (
            # 2010-01-01 00:00:00). As such, we remove the first hour too (with [1:]) to make the DFs match.
            dhw_profile_for_city = pd.concat([self._dhw_profile] * num_days)[1:]
            dhw_profile_for_city = pd.DataFrame(dhw_profile_for_city.values,
                                                columns=dhw_profile_for_city.columns,
                                                index=self.processed_data[k].index)

            self.processed_data[k][heat_demand_for_city.columns] = heat_demand_for_city
            self.processed_data[k]["DHW_hourly_consumption_ratio"] = dhw_profile_for_city["DHW Profile"]
            self.processed_data[k]["season"] = calculate_season(self.processed_data[k])
            self.processed_data[k]["hour"] = self.processed_data[k].index.hour
            self.processed_data[k]["dayofweek"] = self.processed_data[k].index.dayofweek
            self.processed_data[k]["date"] = self.processed_data[k].index.date

    def load_processed_data(self, reload: bool = False):
        if reload or len(self.processed_data) == 0 or self._heat_demand is None or self._dhw_profile is None:
            self._load_all_processed_data()

        return self.processed_data.copy()
