import random
import datetime


class Person:
    def __init__(self, id, initial_station, initial_time, initial_battery):
        self.id = id
        self.initial_station = initial_station
        self.current_station = initial_station
        self.path = []
        self.initial_time = initial_time
        self.initial_battery = initial_battery

    def get_id(self):
        return self.id

    def get_initial_station(self):
        return self.initial_station


def generate_persons(stations, number_of_persons, initial_time):
    persons = []
    operacao = random.choice(["somar", "subtrair"])
    numero = datetime.timedelta(minutes=random.randint(0, 15))
    for i in range(number_of_persons):
        persons.append(Person(i + 1, random.choice(stations).get_id(),
                              initial_time + numero if operacao == "somar" else initial_time - numero,
                              random.randint(10, 100)))
    return persons
