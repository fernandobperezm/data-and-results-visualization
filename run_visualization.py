from src.ECADDataset import ECADMeanTemperatureDataset


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
              & (dataset.mean_temperatures.date >= "2020-01-01")
              & (dataset.mean_temperatures.date < "2021-01-01")
              & (dataset.mean_temperatures.source_id > 0)])
