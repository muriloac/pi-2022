import json

import networkx as nx

import EstacaoMetro
import generator
from datetime import datetime, timedelta
import socket
from time import sleep
import asyncio
from azure.iot.device.aio import IoTHubDeviceClient
import os


async def send_to_azure(device_id, mensagem):
    conn_str = os.getenv("IOTHUB_DEVICE{}_CONNECTION_STRING".format(1))
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)
    await device_client.connect()
    print("Mandando mensagem do dispositivo {}".format(device_id))
    await device_client.send_message(mensagem)
    print("Mensagem enviada do dispositivo {}!".format(device_id))
    await device_client.shutdown()


def send_message(person_id, time, lat, lon, battery, current_station, will_be_late):
    message = {
        "person_id": person_id,
        "time": time,
        "lat": lat,
        "lon": lon,
        "battery": battery,
        "current_station": current_station,
        "will_be_late": will_be_late
    }

    if lat is not None or lon is not None:
        asyncio.run(
            send_to_azure(person_id,
                          json.dumps(message)))
    else:
        asyncio.run(
            send_to_azure(person_id, json.dumps(message)))


def main(message_data, shortest_paths, edges_map_with_weights):
    person_id, time, lat, lon, battery, current_station = message_data.split(',')

    destination = 18850

    # Horario de chegada na estacao final = 19:00
    time_to_arrive = datetime.strptime('1:00:00', '%H:%M:%S')

    remaining_time = generator.analyze_data(destination, current_station, shortest_paths, edges_map_with_weights)

    # Caso o horario atual + tempo restante seja maior que o horario de chegada, o usuario estara atrasado
    # Formate a variavel time para datetime
    # Formate remaining_time para datetime
    # Compare os dois
    will_be_late = datetime.strptime(time, '%H:%M:%S') + timedelta(minutes=remaining_time) > time_to_arrive

    print("Enviando mensagem para o Azure...")
    send_message(person_id, time, lat, lon, battery, current_station, will_be_late)


if __name__ == '__main__':
    # Cria o socket
    server_socket = socket.socket()

    # Obtém o endereço IP do host local
    host = socket.gethostname()

    # Define a porta que o servidor vai escutar
    port = 12345

    # Associa o socket ao endereço e porta especificados
    server_socket.bind((host, port))

    # Configura o socket para escutar conexões
    server_socket.listen()

    print("Criando rede de estacoes...")
    g_map = EstacaoMetro.build_station_network()

    edges = nx.get_edge_attributes(g_map, 'weight')

    print("Calculando os caminhos mais curtos")
    EstacaoMetro.shortest_paths(g_map)
    paths = generator.read_paths()

    print("Servidor iniciado. Aguardando conexões...")

    while True:
        # Aceita uma conexão quando ela for recebida
        connection, address = server_socket.accept()

        print("Conexão recebida de {}".format(address))

        # Recebe dados do cliente
        data = connection.recv(1024)

        # Decodifica os dados recebidos (de bytes para string)
        message = data.decode()

        # get the last line of the message
        received_body = message.splitlines()[-1]

        print("Dados recebidos: {}".format(received_body))

        main(received_body, paths, edges)

        # Envia a linha de status HTTP
        connection.sendall(b"HTTP/1.1 200 OK\r\n\r\n")

        # Envia a mensagem de volta para o cliente
        connection.sendall(message.encode())

        sleep(1)

        # Fecha a conexão
        connection.close()
