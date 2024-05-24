import requests
import constants
import pandas as pd
import csv
from dataclasses import dataclass


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
        output_file = "dog_lover_data.csv"
        headers = ["id", "name", "bred_for", "breed_group", "weight", "height", "life_span", "temperament", "origin", "image"]

        with open(output_file, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for dog in data:
                writer.writerow([dog.id, dog.name, dog.bred_for, dog.breed_group, dog.weight, dog.height, dog.life_span, dog.temperament, dog.origin, dog.image])


class TransformAndLoad:

    # TODO complete transfrom and load class

    def __init__(self, file) -> None:
        self.file = file

    def transform_data(self):
        df = pd.read_csv(self.file)

        # split weight into min and max
        df['weight_min'] = df["weight"].str.extract(r"^(\d+)").fillna(0).astype(int)
        df['weight_max'] = df["weight"].str.extract(r"- (\d+)").fillna(0).astype(int)

        # split height into min and max
        df['height_min'] = df["height"].str.extract(r"^(\d+)").fillna(0).astype(int)
        df['height_max'] = df["height"].str.extract(r"- (\d+)").fillna(0).astype(int)   

        # split life span into min and max
        df['life_span_min'] = df["life_span"].str.extract(r"^(\d+)").fillna(0).astype(int)
        df['life_span_max'] = df["life_span"].str.extract(r"- (\d+)").fillna(0).astype(int)          

        df.to_csv(self.file, index=False)


    def load_data(self):
        pass

def main():

    output_file = "dog_lover_data.csv"

    d = DogAPIHandler()
    d.fetch_dog_data()
    d.process_data()                


if __name__ == "__main__":
    main()