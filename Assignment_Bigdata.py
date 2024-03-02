import requests
import redis
import json
import matplotlib.pyplot as plt
import pandas as pd

class DataImporter:
    def fetch_api_data(self, url):
        try:
            response = requests.get(url)
            data = response.json()
            return data
        except Exception as e:
            print("An error occurred while fetching data:", e)
            return None

    def load_data_to_redis(self, data, redis_host=None, redis_port=None, redis_username=None, redis_password=None, redis_db=None):
        try:
            r = redis.Redis(host=redis_host, port=redis_port, username=redis_username, password=redis_password)
            json_data = json.dumps(data)
            r.set('data_key', json_data)
            print("Data loaded into Redis successfully.")
        except Exception as e:
            print("An error occurred while loading data into Redis:", e)

    def read_data_from_redis(self, redis_host=None, redis_port=None, redis_username=None, redis_password=None, redis_db=None):
        try:
            r = redis.Redis(host=redis_host, port=redis_port, username=redis_username, password=redis_password)
            json_data = r.get('data_key')

            if json_data:
                data = json.loads(json_data)
                return data
            else:
                print("No data found in Redis.")
                return None
        except Exception as e:
            print("An error occurred while reading data from Redis:", e)
            return None

class DataAnalyzer:
    def __init__(self, data):
        self.data = data
        self.df = pd.DataFrame(data)

    def plot_assisted_units(self, town_name):
        town_data = self.df[self.df['Town'] == town_name]
        if not town_data.empty:
            plt.bar(town_data['Year'], town_data['Total Assisted Units'])
            plt.xlabel('Year')
            plt.ylabel('Total Assisted Units')
            plt.title(f'Total Assisted Units in {town_name} Over the Years')
            plt.show()
        else:
            print(f"No data found for {town_name}.")

    def search_town(self, town_name):
        search_result = self.df[self.df['Town'] == town_name]
        return search_result

    def find_min_max_affordable(self):
        is_numeric = pd.api.types.is_numeric_dtype(self.df['Percent Affordable'])

        if is_numeric:
            self.df = self.df.dropna(subset=['Percent Affordable'])
        else:
            try:
                self.df['Percent Affordable'] = pd.to_numeric(self.df['Percent Affordable'], errors='coerce')
                self.df = self.df.dropna(subset=['Percent Affordable'])
            except Exception as e:
                print(f"Error processing 'Percent Affordable': {e}")
                return None

        if not self.df.empty:
            min_affordable = self.df['Percent Affordable'].min()
            max_affordable = self.df['Percent Affordable'].max()

            min_affordable_towns = ', '.join(self.df[self.df['Percent Affordable'] == min_affordable]['Town'].astype(str).to_list())
            max_affordable_towns = ', '.join(self.df[self.df['Percent Affordable'] == max_affordable]['Town'].astype(str).to_list())

            return min_affordable, min_affordable_towns, max_affordable, max_affordable_towns
        else:
            print("No valid data found for 'Percent Affordable'.")
            return None

if __name__ == "__main__":
    data_importer = DataImporter()
    url = "https://apis-ugha.onrender.com/housing"
    housing_data = data_importer.fetch_api_data(url)

    print("Data extraction successful")

    redis_host = 'redis-10613.c321.us-east-1-2.ec2.cloud.redislabs.com'
    redis_port = 10613
    redis_password = 'Bigdata123@'
    redis_db = 'Bigdata'
    username = 'default'
    data_importer.load_data_to_redis(
        housing_data, redis_host=redis_host, redis_port=redis_port, redis_username=username,
        redis_password=redis_password, redis_db=redis_db
    )

    redis_housing_data = data_importer.read_data_from_redis(
        redis_host=redis_host, redis_port=redis_port, redis_username=username,
        redis_password=redis_password, redis_db=redis_db
    )

    if redis_housing_data:
        analyzer = DataAnalyzer(redis_housing_data)

        analyzer.plot_assisted_units("Andover")

        search_result = analyzer.search_town("Bridgewater")
        if not search_result.empty:
            print(f"Data for Bridgewater:")
            print(search_result)
        else:
            print("No data found for Bridgewater.")

        min_affordable, min_town_names, max_affordable, max_town_names = analyzer.find_min_max_affordable()
        if min_affordable is not None:
            print(f"Min Affordable: {min_affordable}, Towns: {min_town_names}")
            print(f"Max Affordable: {max_affordable}, Towns: {max_town_names}")
