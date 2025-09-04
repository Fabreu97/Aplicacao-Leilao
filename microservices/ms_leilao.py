import sys, os
import pika as rabbit
import json
import uuid

def callback_solicitacao_de_leilao(ch, method, properties, body: bytes):
    data = json.loads(body.decode('utf-8'))
    # nome
    # descrição
    # data_inicio
    # data_fim

    # Verificar se a solicitação é valida

    # Gerar um UUID e status ativo se for valido
    data['ID'] = str(uuid.uuid4())
    data['status'] = 'ativo'
    
    # enviar pacote do leilao para todos os clientes
    message = json.dumps(data)

    ch.basic_publish(
        exchange='fanout_leilao',
        routing_key='',
        body=message
    )

def main():
    broker_IP = 'localhost'
    address = rabbit.ConnectionParameters(broker_IP)
    connection = rabbit.BlockingConnection(address)
    channel = connection.channel()

    # Declaração das Exchanges
    channel.exchange_declare(exchange='fanout_leilao', exchange_type='fanout')
    channel.exchange_declare(exchange='direct_leilao', exchange_type='direct')

    # Declaração das Filas
    channel.queue_declare(queue='solicitacao_leilao', exclusive=False)

    # Vinculação das Filas
    channel.queue_bind(queue='solicitacao_leilao', exchange='direct_leilao', routing_key='solicitacao_leilao')

    # Quais filas seram consumidas
    channel.basic_consume(
        queue='solicitacao_leilao',
        on_message_callback=callback_solicitacao_de_leilao,
        auto_ack=True
    )
    print("Inicialização do consumo da fila: solicitacao_leilao . . .")
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupido o MicroServiço de Leilão . . .')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
