import os

import networkx as nx
import csv

import EstacaoMetro
import generator
import Person
from datetime import datetime
import multiprocessing


def main():
    print("Criando estacoes...")
    stations = EstacaoMetro.criar_estacoes()

    print("Criando rede de estacoes...")
    g_map = EstacaoMetro.build_station_network()

    # edges_map, colors = zip(*nx.get_edge_attributes(g_map, 'color').items())
    edges_map_with_weights = nx.get_edge_attributes(g_map, 'weight')
    #
    # print("Desenhando o mapa do metro")
    # EstacaoMetro.draw_network(g_map, edges_map, color=colors)

    print("Calculando os caminhos mais curtos")
    EstacaoMetro.shortest_paths(g_map)
    shortest_paths = generator.read_paths()

    print("Gerando pessoas")
    # datetime 2022-12-05 18:00:00
    persons = Person.generate_persons(stations, 60, datetime.now())

    # print("Quantos dispositivos vão ser simulados? MAX 60")
    # qtd_simulados = int(input('Input: '))
    qtd_simulados = 45

    pool_handler(qtd_simulados, 18850, stations, shortest_paths, persons, edges_map_with_weights)


def pool_handler(qtd_simulados, destination, stations, shortest_paths, persons, edges_map_with_weights):
    p = multiprocessing.Pool(processes=qtd_simulados,
                             initializer=generator.__init__, initargs=[multiprocessing.Lock()])
    data = []
    for i in range(qtd_simulados):
        data.append((destination, stations, shortest_paths, persons[i], edges_map_with_weights))
    with open('resources/out/data.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['person_id', 'time', 'lat', 'lon', 'battery', 'current_station'])
        csvfile.close()
    for _ in p.imap_unordered(generator.generate_data, data):
        pass


if __name__ == '__main__':
    main()
