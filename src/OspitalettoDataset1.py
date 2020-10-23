import os
from typing import Dict, Optional

import numpy as np
import pandas as pd


class OspitalettoDataset(object):
    """
    This dataset contains hourly measurements of different attributes like air temperature, dewp, and more.
    """
    OSPITALETTO: str ="ospitaletto_italy"
    LONDON_UK: str = "london_uk"
    MADRID_SPA: str = "madrid_spain"
    ROME_IT: str = "rome_italy"
    STUTTGART_GER: str = "stuttgart_germany"

    def __init__(self):
        self._dataset_local_extract_path = os.path.join(".", "data", "Ospitaletto")

        self._heat_demand_path = os.path.join(self._dataset_local_extract_path, "heat_demand.xlsx")
        self._dhw_profile_path = os.path.join(self._dataset_local_extract_path, "dhw_random_profile.xlsx")
        
        self._osp_dataset_path = os.path.join(self._dataset_local_extract_path, "Osp.xls")
        self._london_dataset_path = os.path.join(self._dataset_local_extract_path, "London.xls")
        self._madrid_dataset_path = os.path.join(self._dataset_local_extract_path, "Madrid.xls")
        self._rome_dataset_path = os.path.join(self._dataset_local_extract_path, "Rome.xls")
        self._stuttgart_dataset_path = os.path.join(self._dataset_local_extract_path, "Stuttgart.xls")
        
        self.processed_osp_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                             "processed",
                                                             "osp_data")
        self.processed_london_uk_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                             "processed",
                                                             "london_uk_data")
        self.processed_madrid_spa_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                              "processed",
                                                              "madrid_spa_data")
        self.processed_rome_it_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                           "processed",
                                                           "rome_it_data")
        self.processed_stuttgart_ger_dataset_path = os.path.join(self._dataset_local_extract_path,
                                                                 "processed",
                                                                 "stuttgart_ger_data")

        self._heat_demand: Optional[pd.DataFrame] = None
        self._dhw_profile: Optional[pd.Series] = None

        self.data: Dict = dict()
        self.processed_data: Dict = dict()

    def _load_all_data(self):
        def calculate_season(data: pd.DataFrame):
            winter_ends = "2017-03-20"
            spring_ends = "2017-06-20"
            summer_ends = "2017-09-22"
            fall_ends = "2017-12-21"

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
        
        keys_with_paths = [(self.OSPITALETTO, self._osp_dataset_path),
                           (self.LONDON_UK, self._london_dataset_path),
                           (self.MADRID_SPA, self._madrid_dataset_path),
                           (self.ROME_IT, self._rome_dataset_path),
                           (self.STUTTGART_GER, self._stuttgart_dataset_path)]
        
        self._heat_demand = pd.read_excel(self._heat_demand_path)
        self._dhw_profile = pd.read_excel(self._dhw_profile_path, index_col="Hour")

        for k, file_path in keys_with_paths:
            data = pd.read_excel(file_path,
                                 header=0,
                                 names=["hourofyear", "air_temp"],
                                 usecols=["hourofyear", "air_temp"],
                                 dtype={"hourofyear": np.int32,
                                        "air_temp": np.float32})

            data["timestamp"] = pd.date_range("2017-01-01", freq="H", periods=data.shape[0])
            data = data.set_index("timestamp")
            
            num_rows_in_processed_data = data.shape[0]
            num_days = data.index.dayofyear.max()

            # We select the current city (determined by k and matched with the city_key column)
            # Then we repeat the rows in the dataset for num_rows_in_processed_data times
            # Third, we reindex the dataframe using the self.processed_data[k] index
            # Lastly, we concat the heat demand columns into the processed data
            heat_demand_for_city = self._heat_demand[self._heat_demand["city_key"] == k]
            heat_demand_for_city = pd.concat([heat_demand_for_city] * num_rows_in_processed_data)
            heat_demand_for_city = pd.DataFrame(heat_demand_for_city.values,
                                                columns=heat_demand_for_city.columns,
                                                index=data.index).astype({"DHW_cons": np.float32,
                                                                          "SH_cons": np.float32,
                                                                          "SFH_bldg_tot": np.float32,
                                                                          "MFH_bldg_tot": np.float32,
                                                                          "%SH_y": np.float32,
                                                                          "%DHW_y": np.float32})

            # The DHW profile is constant through the year but changes during the day (however, it keeps the same
            # values for the same hour in different days). The InsPire dataset contains an extra hour (2018-01-01
            # 00:00:00). We have to include the first value in the first position too
            dhw_profile_for_city = pd.concat([self._dhw_profile] * num_days)
            dhw_profile_for_city = pd.concat([dhw_profile_for_city, dhw_profile_for_city.head(1)])
            dhw_profile_for_city = pd.DataFrame(dhw_profile_for_city.values,
                                                columns=dhw_profile_for_city.columns,
                                                index=data.index).astype({"DHW Profile": np.float32})

            data[heat_demand_for_city.columns] = heat_demand_for_city
            data["DHW_hourly_consumption_ratio"] = dhw_profile_for_city["DHW Profile"]
            data["season"] = calculate_season(data)
            data["date"] = data.index.date
            data["month"] = data.index.month
            data["dayofyear"] = data.index.dayofyear
            data["dayofweek"] = data.index.dayofweek
            data["hourofyear"] = (data.index.dayofyear - 1) * 24 + (data.index.hour + 1)
            data["hour"] = data.index.hour

            self.data[k] = data

    def load_data(self, reload: bool = False) -> Dict:
        if reload or self._heat_demand is None or self._dhw_profile is None:
            self._load_all_data()

        return self.data.copy()

    def _load_all_processed_data(self):
        keys_with_paths = [(self.LONDON_UK, self.processed_london_uk_dataset_path),
                           (self.MADRID_SPA, self.processed_madrid_spa_dataset_path),
                           (self.ROME_IT, self.processed_rome_it_dataset_path),
                           (self.STUTTGART_GER, self.processed_stuttgart_ger_dataset_path)]
        columns_from_kw_to_mw = ['heat_source1', 'heat_source2', 'heat_aquifer', "E_el", "Total_consumption", "Total_consumption_fit",]
        
        for k, file_path in keys_with_paths:
            data = pd.read_parquet(path=f"{file_path}.parquet",
                                   engine="pyarrow")
            data[columns_from_kw_to_mw] = data[columns_from_kw_to_mw] / 1000
            self.processed_data[k] = data

    def load_processed_data(self, reload: bool = False):
        if reload or len(self.processed_data) == 0:
            self._load_all_processed_data()

        return self.processed_data.copy()


