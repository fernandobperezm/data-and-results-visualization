from src.ECADDataset import ECADMeanTemperatureDataset
from src.NOAA2010Dataset import NOAA2010Dataset


if __name__ == "__main__":
    dataset = ECADMeanTemperatureDataset()

    dataset.load_all_stations()
    print(dataset.stations)
    print(dataset.stations.info())

    dataset.load_all_elements()
    print(dataset.elements)
    print(dataset.elements.info())

    dataset.load_all_sources()
    print(dataset.sources)
    print(dataset.sources.info())

    dataset.load_mean_temperature_files_by_station_id(station_ids=[18, 3, 1995, 9, 8, 1994, 21, 23, 6, 10])
    print(dataset.mean_temperatures)
    print(dataset.mean_temperatures.info())

    print(dataset.mean_temperatures[
              (dataset.mean_temperatures.mean_temperature >= 0)
              & (dataset.mean_temperatures.mean_temperature <= 200)
              & (dataset.mean_temperatures.date >= "2010-01-01")
              & (dataset.mean_temperatures.date < "2011-01-01")
              & (dataset.mean_temperatures.source_id > 0)])

    noaa2010dataset = NOAA2010Dataset()

    noaa2010dataset.load_all_data()
    print(noaa2010dataset.data)
    print(noaa2010dataset.data[["date", "hourly_temperature"]].head(50))
    print(noaa2010dataset.data[["date", "hourly_temperature"]].tail(50))
    print(noaa2010dataset.data.info())

    print(noaa2010dataset.data[
              (noaa2010dataset.data.hourly_temperature >= 0)
              & (noaa2010dataset.data.hourly_temperature <= 20)])

