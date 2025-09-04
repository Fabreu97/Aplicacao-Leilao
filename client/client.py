import pika as rabbit
import sys, os
import threading
import datetime
import time
import json

def envio_leilao():
    time.sleep(3)
    broker_IP = 'localhost'
    address = rabbit.ConnectionParameters(broker_IP)
    connection = rabbit.BlockingConnection(address)
    channel = connection.channel()

    # Construção do pacote para requisição
    package = {}
    data_inicio = datetime.datetime.now()
    data_fim = data_inicio + + datetime.timedelta(hours=2)
    package['name'] = 'Fernando'
    package['descrição'] = 'Leilao do meu coração'
    package['data_inicio'] = data_inicio.strftime("%d/%m/%Y %H:%M:%S")
    package['data_fim'] = data_fim.strftime("%d/%m/%Y %H:%M:%S")
    request = json.dumps(package)

    # declarar as Exchange
    channel.exchange_declare(exchange='direct_leilao', exchange_type='direct')

    channel.basic_publish(
        exchange='direct_leilao',
        routing_key='solicitacao_leilao',
        body=request
    )
    connection.close()


def callback_leilao_inicializado(ch, method, properties, body: bytes):
    print(body.decode('utf-8'))

def main():
    broker_IP = 'localhost'
    address = rabbit.ConnectionParameters(broker_IP)
    connection = rabbit.BlockingConnection(address)
    channel = connection.channel()

    # Declarar as exchange
    channel.exchange_declare(exchange='fanout_leilao', exchange_type='fanout')
    channel.exchange_declare(exchange='direct_leilao', exchange_type='direct')

    # Declarar a fila fanout a ser escutada
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    # Declarar a Vinculação
    channel.queue_bind(queue=queue_name, exchange='fanout_leilao', routing_key='')

    # Definir as filas que vou escuta
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback_leilao_inicializado,
        auto_ack=True
    )
    print("Inicialização do consumo da fila: leilao inicializado . . .")
    channel.start_consuming()

if __name__ == '__main__':
    try:
        t1 = threading.Thread(target=main)
        t2 = threading.Thread(target=envio_leilao)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)


