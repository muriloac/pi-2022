import csv
import datetime
import random
import time
import threading
import socket

from event_variables import CLIMA, CHUVA


# url = "http://192.168.15.49:12345"

def __init__(lock):
    global starting
    starting = lock


def enviar_dados(message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('192.168.15.49', 8080))
    s.sendall(message.encode("UTF-8"))
    s.close()


def generate_data(data):
    starting.acquire()
    threading.Timer(3, starting.release).start()
    destination, stations, shortest_paths, person, edges_map_with_weights = data
    person.path = get_path_for_person(person, shortest_paths, destination)

    person_data = simulate_path_with_time(person, edges_map_with_weights, stations, random.choice(CLIMA),
                                          random.choice(CHUVA))

    with open('resources/out/data.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        for i in range(len(person_data)):
            if person_data[i][2] is not None or person_data[i][2] is not None:
                writer.writerow(
                    [person_data[i][0], person_data[i][1].strftime("%Y-%m-%d %H:%M:%S"), person_data[i][2][0],
                     person_data[i][2][1],
                     person_data[i][3],
                     person_data[i][4]])
            else:
                writer.writerow(
                    [person_data[i][0], person_data[i][1].strftime("%Y-%m-%d %H:%M:%S"), None, None, person_data[i][3],
                     person_data[i][4]])
            csvfile.flush()

        last_time = None
        current_time = None
        print("Iniciando envio de dados do dispositivo {}".format(person.id))
        for i in range(len(person_data)):
            if last_time is None:
                last_time = person_data[i][1]
                current_time = person_data[i][1]
            else:
                last_time = current_time
                current_time = person_data[i][1]
            time_to_sleep = (
                    datetime.datetime.combine(datetime.date.min, current_time.time()) - datetime.datetime.combine(
                datetime.date.min, last_time.time())).seconds
            if time_to_sleep > 0:
                print("Esperando {} segundos".format(time_to_sleep))
                time.sleep(time_to_sleep)
            if person_data[i][2] is not None or person_data[i][2] is not None:
                message = ",".join(
                    map(str, [person_data[i][0], person_data[i][1].strftime("%Y-%m-%d %H:%M:%S"), person_data[i][2][0],
                              person_data[i][2][1],
                              person_data[i][3],
                              person_data[i][4]]))
            else:
                message = ",".join(map(str,
                                       [person_data[i][0], person_data[i][1].strftime("%Y-%m-%d %H:%M:%S"), None, None,
                                        person_data[i][3],
                                        person_data[i][4]]))

            print("Enviando dados do dispositivo {}".format(person.id))
            print(message)
            try:
                enviar_dados(message)
            except Exception as e:
                print(e)


def generate_point_on_path_ratio(lat1, lon1, lat2, lon2, ratio):
    lat = lat1 + (lat2 - lat1) * ratio
    lon = lon1 + (lon2 - lon1) * ratio
    return round(lat, 3), round(lon, 3)


def read_climate_data(climate_data_file='resources/clima_chuva.csv'):
    with open(climate_data_file, 'r', encoding='latin-1') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        next(reader)
        aux = []
        for row in reader:
            aux.append([row[0], row[1], row[7]])
    return aux


def read_paths(shortest_paths_csv='resources/shortest_paths.csv'):
    with open(shortest_paths_csv, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader)
        aux = []
        for row in reader:
            aux.append([row[0], row[1], row[2]])
    return aux


def get_path_for_person(person, shortest_paths, destination):
    shortest_path = []
    for path in shortest_paths:
        if path[1] == str(destination) and path[0] == str(person.get_initial_station()):
            shortest_path = path[2]
            break

    return shortest_path


def simulate_path_with_time(person, edges_map_with_weights, stations, clima, chuva):
    path = person.path
    time = person.initial_time
    time_list = []
    try:
        path = path.replace("'", '').replace('[', '').replace(']', '').replace(' ', '').split(',')
    except:
        print(path)
    battery = person.initial_battery

    for i in range(len(path) - 1):
        edge = (path[i], path[i + 1])
        time_edge = edges_map_with_weights.get(edge) if edges_map_with_weights.get(
            edge) is not None else edges_map_with_weights.get(
            (path[i + 1], path[i]))
        current_station = (x for x in stations if x.id == path[i + 1]).__next__()
        last_station = (x for x in stations if x.id == path[i]).__next__()

        if chuva == 'sim' and clima == 'quente':
            time_edge = time_edge * 1.7
        elif clima == 'quente':
            time_edge = time_edge * 1.2
        elif chuva == 'sim':
            time_edge = time_edge * 1.5

        ratio = random.random()
        random_int = random.randint(0, 100)
        battery = round(battery - 0.005, 3)
        time = time + datetime.timedelta(minutes=time_edge)
        if random_int < 5:
            time_list.append((person.id, time,
                              generate_point_on_path_ratio(last_station.lat, last_station.lon, current_station.lat,
                                                           current_station.lon, ratio), battery, current_station.id))
        elif 10 > random_int >= 5:
            time_list.append((person.id, time, None, battery, current_station.id))
        else:
            lat, lon = current_station.get_coordenadas()
            time_list.append(
                (person.get_id(), time, (round(lat, 3), round(lon, 3)), battery, current_station.id))

    return time_list
