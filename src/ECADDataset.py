import io
import zipfile
import requests
import os
import re
from typing import Optional, List, Union

import numpy as np
import pandas as pd


class ECADMeanTemperatureDataset(object):
    def __init__(self):
        self._dataset_url = "https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_tg.zip"
        self._dataset_local_extract_path = os.path.join(".", "data", "ECADMeanTemperatureDataset")

        self._dataset_stations_path = os.path.join(self._dataset_local_extract_path, "stations.txt")
        self._dataset_elements_path = os.path.join(self._dataset_local_extract_path, "elements.txt")
        self._dataset_sources_path = os.path.join(self._dataset_local_extract_path, "sources.txt")

        self.stations: Optional[pd.DataFrame] = None
        self.elements: Optional[pd.DataFrame] = None
        self.sources: Optional[pd.DataFrame] = None
        self.mean_temperatures: Optional[pd.DataFrame] = None

        self.dataset_exists_locally = (os.path.exists(self._dataset_local_extract_path)
                                       and os.path.isdir(self._dataset_local_extract_path))

    def _save_dataset_on_local_disk(self):
        dataset_zip_content = requests.get(self._dataset_url, stream=True).content
        with zipfile.ZipFile(file=io.BytesIO(dataset_zip_content), mode="r") as zip_dataset:
            zip_dataset.extractall(self._dataset_local_extract_path)

        self.dataset_exists_locally = True

    def load_all_stations(self):
        if not self.dataset_exists_locally:
            self._save_dataset_on_local_disk()

        self.stations = pd.read_csv(self._dataset_stations_path,
                                    sep=",",
                                    skiprows=17,
                                    header=0,
                                    names=["station_id", "station_name", "country_code", "latitude", "longitude",
                                           "height"],
                                    dtype={"station_id": np.int32,
                                           "station_name": "object",
                                           "country": "object",
                                           "latitude": "object",
                                           "longitude": "object",
                                           "height": np.int32})

    def load_all_elements(self):
        if not self.dataset_exists_locally:
            self._save_dataset_on_local_disk()

        self.elements = pd.read_csv(self._dataset_elements_path,
                                    sep=",",
                                    skiprows=14,
                                    header=0,
                                    error_bad_lines=False,
                                    warn_bad_lines=True,
                                    names=["element_id", "description", "unit"],
                                    dtype={"element_id": "object",
                                           "description": "object",
                                           "unit": "object"})

    def load_all_sources(self):
        if not self.dataset_exists_locally:
            self._save_dataset_on_local_disk()

        self.sources = pd.read_csv(self._dataset_sources_path,
                                   sep=",",
                                   skiprows=23,
                                   header=0,
                                   error_bad_lines=False,
                                   warn_bad_lines=True,
                                   parse_dates=["start_date", "end_date"],
                                   infer_datetime_format=True,
                                   na_values=["    -"],
                                   names=["station_id",
                                          "source_id",
                                          "source_name",
                                          "country_code",
                                          "latitude",
                                          "longitude",
                                          "height",
                                          "element_id",
                                          "start_date",
                                          "end_date",
                                          "participant_id",
                                          "participant_name"],
                                   dtype={"station_id": np.int32,
                                          "source_id": np.int32,
                                          "source_name": "object",
                                          "country_code": "object",
                                          "latitude": "object",
                                          "longitude": "object",
                                          "height": np.int32,
                                          "element_id": "object",
                                          "participant_id": np.float32,
                                          # It is an integer, but sometimes this value is not present.
                                          "participant_name": "object"})

    def load_mean_temperature_files_by_station_id(self, station_ids: Union[List[int], int]):
        """

        :param station_ids: A list of positive integers representing the station IDs to read the data. If you wish to
        read the data from all stations, then set this argument as -1.
        :return: Nothing. The method loads the requested mean temperature data inside the mean_temperature_measurements
        attribute.
        """

        def files_to_pandas(filenames_to_load: List[str]):
            # Iterate over match objects instead of filenames
            for filename in filenames_to_load:
                full_filename_path = os.path.join(self._dataset_local_extract_path, filename)
                print(f"Processing filename: {full_filename_path}")
                yield (pd.read_csv(full_filename_path,
                                   sep=",",
                                   skiprows=20,
                                   header=0,
                                   parse_dates=["date"],
                                   infer_datetime_format=True,
                                   names=["station_id",
                                          "source_id",
                                          "date",
                                          "mean_temperature",
                                          "quality_code"],
                                   dtype={"station_id": np.int32,
                                          "source_id": np.int32,
                                          "mean_temperature": np.int32,
                                          "quality_code": "category"})
                       )

        if not self.dataset_exists_locally:
            self._save_dataset_on_local_disk()

        all_files = os.listdir(self._dataset_local_extract_path)
        if station_ids == -1:
            print("Warning: Loading all temperature files uses more than 4GiB of RAM and takes a lot of time to load.")

            # Matches Mean Temperature files having the name TG_STAIDXXXXXX.txt
            # where XXXXXX represents 6 digits.
            regex = re.compile(r'^TG_STAID(\d{6}).txt$')

            # Filter out filenames not matching with the expected name
            filenames = sorted(filter(regex.match, all_files), reverse=False)

        elif isinstance(station_ids, list) and all(map(lambda station_id: station_id > 0, station_ids)):
            station_ids_filenames = [f"TG_STAID{str(station_id).zfill(6)}.txt"
                                     for station_id in station_ids]

            filenames = sorted(set(station_ids_filenames).intersection(all_files))
        else:
            raise ValueError("Value of 'station_ids' is invalid. Pass -1 to read all temperature files or a list of "
                             "positive integers to filter them.")

        self.mean_temperatures = pd.concat(files_to_pandas(filenames))

        # By the data docs, invalid measures have the quality_code on 9.
        self.mean_temperatures = self.mean_temperatures[self.mean_temperatures.quality_code != 9]





