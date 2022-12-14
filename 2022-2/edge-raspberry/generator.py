import csv
import asyncio
from azure.iot.device.aio import IoTHubDeviceClient


def analyze_data(destination, current_station, shortest_paths, edges_map_with_weights):

    # Ache o menor caminho entre a estação atual e a estação de destino
    person_path = get_path(current_station, shortest_paths, destination)

    # Calcule o tempo restante de viagem
    remaining_time = calculate_remaining_time(current_station, person_path, edges_map_with_weights)

    return remaining_time


def read_paths(shortest_paths_csv='resources/shortest_paths.csv'):
    with open(shortest_paths_csv, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader)
        aux = []
        for row in reader:
            aux.append([row[0], row[1], row[2]])
    return aux


def get_path(current_station, shortest_paths, destination):
    shortest_path = []
    for path in shortest_paths:
        if path[1] == str(destination) and path[0] == str(current_station):
            shortest_path = path[2]
            break

    return shortest_path


# Função para calcular o tempo restante de viagem em um dia frio e sem chuva
# Irá receber a estaçao atual, o caminho, o mapa de arestas e as estações
# Retorne um valor em minutos
def calculate_remaining_time(current_station, person_path, edges_map_with_weights):
    time_list = []
    path = person_path.replace("'", '').replace('[', '').replace(']', '').replace(' ', '').split(',')

    # Ache a posição da estação atual no caminho
    current_station_index = path.index(current_station)

    # Percorra o caminho a partir da estação atual
    for i in range(current_station_index, len(path) - 1):
        edge = (path[i], path[i + 1])
        time_edge = edges_map_with_weights.get(edge) if edges_map_with_weights.get(
            edge) is not None else edges_map_with_weights.get(
            (path[i + 1], path[i]))
        time_list.append(time_edge)

    # Retorne a soma dos tempos
    return sum(time_list)
