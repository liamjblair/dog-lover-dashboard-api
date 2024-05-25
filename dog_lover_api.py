import requests
import constants
import pandas as pd
import csv
from dataclasses import dataclass
from sqlalchemy import create_engine


@dataclass
class DogDetails:
    id: int
    name: str
    bred_for: str = "Unknown"
    breed_group: str = "Unkown"
    weight: str = "Unknown"
    height: str = "Unknown"
    life_span: str = "Unknown"
    temperament: str = "Unknown"
    origin: str = "Unknown"
    image: str = "Not found"

class DogAPIHandler:

    def fetch_dog_data(self):
        headers = {"x-api-key": constants.DOGAPI}
        base_url = "https://api.thedogapi.com/v1/"
        endpoint = "breeds"
        url = base_url + endpoint
        try:
            r = requests.get(url, headers)
            self.dogs = r.json()
        except requests.RequestException as e:
            print(f"Error processing HTTP request - {e}")

    def process_data(self):
        image_url = "https://cdn2.thedogapi.com/images/"

        self.dog_data =[]
        for dog in self.dogs:
            dogs = DogDetails(
                id = dog.get("id"),
                name = dog.get("name"),
                bred_for = dog.get("bred_for", "Unknown"),
                breed_group = dog.get("breed_group", "Unknown"),
                weight = dog.get("weight", {}).get("metric", "Unknown"),
                height = dog.get("height", {}).get("metric", "Unknown"),
                life_span = dog.get("life_span", "Unknown"),
                temperament = dog.get("temperament", "Unknown"),
                origin = dog.get("origin", "Unknown"),
                image = image_url + dog.get("reference_image_id", "Not found") + ".jpg"
            )
            self.dog_data.append(dogs)
        self.write_data(self.dog_data)
        
    def write_data(self, data):
        output_file = "dog_lover_data_input.csv"
        headers = ["id", "name", "bred_for", "breed_group", "weight", "height", "life_span", "temperament", "origin", "image"]

        with open(output_file, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for dog in data:
                writer.writerow([dog.id, dog.name, dog.bred_for, dog.breed_group, dog.weight, dog.height, dog.life_span, dog.temperament, dog.origin, dog.image])


class DogTransformAndLoad:

    def __init__(self, 
                 db_user, 
                 db_password, 
                 db_host, 
                 db_port, 
                 db_name, 
                 db_table, 
                 input_file):
        
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_table = db_table
        self.input_file = input_file

    def transform_data(self):
        df = pd.read_csv(self.input_file, encoding="Windows-1252")

        # split weight into min and max
        df['weight_min'] = df["weight"].str.extract(r"^(\d+)").fillna(0).astype(int)
        df['weight_max'] = df["weight"].str.extract(r"- (\d+)").fillna(0).astype(int)

        # split height into min and max
        df['height_min'] = df["height"].str.extract(r"^(\d+)").fillna(0).astype(int)
        df['height_max'] = df["height"].str.extract(r"- (\d+)").fillna(0).astype(int)   

        # split life span into min and max
        df['life_span_min'] = df["life_span"].str.extract(r"^(\d+)").fillna(0).astype(int)
        df['life_span_max'] = df["life_span"].str.extract(r" (\d+) ").fillna(0).astype(int)        

        # create output file
        self.output_file = "dog_lover_data_output.csv"
        df.to_csv(self.output_file, index=False)

    def load_data(self):

        """load data in target db table"""

        df = pd.read_csv(self.output_file)

        try:
            conn_str = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
            db_engine = create_engine(conn_str)

            # Write data to PostgreSQL database
            df.to_sql(self.db_table, db_engine, if_exists="replace", index=False)

            db_engine.dispose()
        except Exception as e:
            print(f"Following error ocurring attemping to load data into posgres db - {e}")


def main():
    fetch_dogs = DogAPIHandler()
    fetch_dogs.fetch_dog_data()
    fetch_dogs.process_data()                

    input_file = "dog_lover_data_input.csv"
    db_tbl = "dog_lover_data"
    db_user = constants.DB_USER
    db_password = constants.DB_PASSWORD
    db_host = constants.DB_HOST
    db_port = constants.DB_PORT
    db_name = constants.DB_NAME

    load_dogs = DogTransformAndLoad(db_user,
                                    db_password,
                                    db_host,
                                    db_port,
                                    db_name,
                                    db_tbl,
                                    input_file)
    load_dogs.transform_data()
    load_dogs.load_data()

if __name__ == "__main__":
    main()