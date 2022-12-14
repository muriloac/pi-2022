# don't use datetime
import urequests
import machine
import network
import micropythontime
import ntptime
import utime
import gc
import socket

# Transforma esses dados em um array
# Coloque aspas simples em todas as strings
# person_id,time,lat,lon,battery,current_station
# 1,2022-12-09 17:51:00,-23.656,-46.72,44.995,18906
# 1,2022-12-09 17:54:36,-23.654,-46.71,44.99,19040
# 1,2022-12-09 17:58:12,-23.65,-46.704,44.985,7206877
# 1,2022-12-09 18:01:48,-23.642,-46.699,44.98,7206943
# 1,2022-12-09 18:05:24,,,44.975,7206944
# 1,2022-12-09 18:09:00,-23.627,-46.688,44.97,7206945
# 1,2022-12-09 18:12:36,-23.618,-46.682,44.965,3407190
# 1,2022-12-09 18:17:24,-23.61,-46.669,44.96,5306691
# 1,2022-12-09 18:21:00,-23.603,-46.662,44.955,5306694
# 1,2022-12-09 18:25:48,-23.598,-46.653,44.95,5306695
# 1,2022-12-09 18:29:24,-23.598,-46.646,44.945,9206548
# 1,2022-12-09 18:33:00,-23.599,-46.637,44.94,18856
# 1,2022-12-09 18:35:24,-23.589,-46.635,44.935,18857
# 1,2022-12-09 18:37:48,-23.581,-46.638,44.93,18860
# 1,2022-12-09 18:43:48,-23.575,-46.641,44.925,18861
# 1,2022-12-09 18:46:12,-23.568,-46.649,44.92,18858
# 1,2022-12-09 18:49:48,-23.564,-46.654,44.915,18859
# 1,2022-12-09 18:53:24,-23.558,-46.66,44.91,18850

data = [
    ['1', '0', '-23.656', '-46.72', '44.995', '18906'],
    ['1', '3', '-23.654', '-46.71', '44.99', '19040'],
    ['1', '4', '-23.65', '-46.704', '44.985', '7206877'],
    ['1', '3', '-23.642', '-46.699', '44.98', '7206943'],
    ['1', '4', '', '', '44.975', '7206944'],
    ['1', '4', '-23.627', '-46.688', '44.97', '7206945'],
    ['1', '3', '-23.618', '-46.682', '44.965', '3407190'],
    ['1', '5', '-23.61', '-46.669', '44.96', '5306691'],
    ['1', '4', '-23.603', '-46.662', '44.955', '5306694'],
    ['1', '4', '-23.598', '-46.653', '44.95', '5306695'],
    ['1', '4', '-23.598', '-46.646', '44.945', '9206548'],
    ['1', '4', '-23.599', '-46.637', '44.94', '18856'],
    ['1', '2', '-23.589', '-46.635', '44.935', '18857'],
    ['1', '2', '-23.581', '-46.638', '44.93', '18860'],
    ['1', '6', '-23.575', '-46.641', '44.925', '18861'],
    ['1', '3', '-23.568', '-46.649', '44.92', '18858'],
    ['1', '3', '-23.564', '-46.654', '44.915', '18859'],
    ['1', '4', '-23.558', '-46.66', '44.91', '18850']
]


# Função que calcula a data e hora atual, mudando o fuso horário para o Brasil (GMT-3)
def calcularTempoEDataAtual():
    response_datetime = urequests.get('http://worldtimeapi.org/api/timezone/America/Sao_Paulo')
    data_atual = response_datetime.json()
    response_datetime.close()
    # reset urequests
    gc.collect()
    data_atual = data_atual['datetime']
    data_atual = data_atual.split('T')
    data_atual = data_atual[0] + ' ' + data_atual[1].split('.')[0]
    gc.collect()
    utime.sleep(1)
    return data_atual


ntptime.host = "br.pool.ntp.org"
ntptime.settime()

# Configura o nome da rede Wi-Fi e a senha
SSID = "AlamsacQuarto"
password = "murilo14"

# Cria o objeto station que permite conectar-se à rede Wi-Fi
station = network.WLAN(network.STA_IF)
ap_if = network.WLAN(network.AP_IF)
ap_if.active(False)

# Verifica se a interface de rede está ativa
if station.active():
    # Desconecta da rede atual
    station.disconnect()

# Configura a interface de rede para se conectar à rede Wi-Fi especificada
station.active(True)
station.connect(SSID, password)

# Verifica se a interface de rede está conectada à rede Wi-Fi
while not station.isconnected():
    print("Connecting to network...")
    utime.sleep(1)
    pass

# Imprime o endereço IP da interface de rede
print("Connected to WiFi. IP address: ", station.ifconfig()[0])

# Define a URL do servidor
url = "http://192.168.15.49:8080/"

# Configura o pino D3 como entrada
flash_button = machine.Pin(0, machine.Pin.IN)

# Configura o pino D0 como saída
led = machine.Pin(2, machine.Pin.OUT)

# Loop infinito
while True:
    # Verifica se o botão foi pressionado
    if flash_button.value() == 0:
        # Acende a LED
        led.value(1)

        # Loop sobre as linhas da lista "data"
        for i in range(len(data) - 1):
            # Pega a hora atual da linha atual
            current_time = data[i][1]

            # Calcula o intervalo de tempo até a próxima hora na lista
            if i < len(data) - 1:
                next_time = data[i + 1][1]
                time_delta = int(next_time) - int(current_time)
            else:
                # Se esta for a última linha, não há próxima hora para calcular o intervalo
                time_delta = 0
            print("time_delta: ", time_delta)

            # Coloca o tempo atual e dia atual no lugar da hora ajustando para o fuso horário de São Paulo
            # Não use .strftime() pois ele não funciona no MicroPython
            data[i][1] = calcularTempoEDataAtual()

            # Transforma a linha em uma string, juntando cada elemento da lista com uma vírgula
            print(data[i])
            message = ",".join(data[i])

            try:
                # Envia a mensagem para o servidor mostrando para onde a mensagem foi enviada
                print("Sending message to server: ", message)
                response = urequests.post(url, data=message)

                # Imprime a resposta do servidor
                print("Response from server: ", response.text)

                # Fecha a conexão
                response.close()

                gc.collect()

                # Aguarda o intervalo de tempo calculado até a próxima hora antes de enviar a próxima mensagem
                sleep_time = time_delta * 60

                if sleep_time > 0:
                    print("Sleeping for ", sleep_time, " seconds")
                    utime.sleep(sleep_time)
            except Exception as e:
                # Imprime qualquer erro que ocorra
                print("An error occurred: ", e)

        # Apaga a LED
        led.value(0)
