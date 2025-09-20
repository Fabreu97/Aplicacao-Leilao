import sys, os
import pika as rabbit
import json

LEILAO_INICIALIZADO = 0
LEILAO_FINALIZADO = 2
LANCE_VALIDO = 3
LANCE_MENOR = 4

def callback_lance_valido(ch, method, properties, body: bytes):
    msg = json.loads(body.decode('utf-8'))
    # ID_leilao
    # ID_usuario
    # lance
    # type
    # Apenas repassando ao topico correto
    print("*" * 22)
    print("LANCE VALIDO: ")
    for key, value in msg.items():
        print(f"{key}: {value}")
    topic = msg['ID_leilao']
    topic = f"leilao.{topic}"

    ch.basic_publish(
        exchange='topic_notificacao',
        routing_key=topic,
        body=body
    )


def callback_leilao_vencedor(ch, method, properties, body: bytes):
    msg = json.loads(body.decode('utf-8'))
    topic = msg['ID_leilao']
    topic = f'leilao.{topic}'

    print("*" * 22)
    print("Leilao Vencedor: ")
    for key, value in msg.items():
        print(f"{key}: {value}")
    topic = msg['ID_leilao']
    topic = f"leilao.{topic}"

    ch.basic_publish(
        exchange='topic_notificacao',
        routing_key=topic,
        body=body
    )


def main():
    broker_IP = 'localhost'
    address = rabbit.ConnectionParameters(broker_IP)
    connection = rabbit.BlockingConnection(address)
    channel = connection.channel()

    # Declaração dos Exchanges
    channel.exchange_declare(exchange='direct_lance', exchange_type='direct')
    channel.exchange_declare(exchange='topic_notificacao', exchange_type='topic')

    # Declaração das filas a serem escutadas
    channel.queue_declare(queue='lance_valido', exclusive=False)
    channel.queue_declare(queue='leilao_vencedor', exclusive=False)

    # Vincular as filas
    channel.queue_bind(queue='lance_valido', exchange='direct_lance', routing_key='lance_valido')
    channel.queue_bind(queue='leilao_vencedor', exchange='direct_lance', routing_key='leilao_vencedor')

    # Filas que serão consumidas
    channel.basic_consume(
        queue='lance_valido',
        on_message_callback=callback_lance_valido,
        auto_ack=True
    )

    channel.basic_consume(
        queue='leilao_vencedor',
        on_message_callback=callback_leilao_vencedor,
        auto_ack=True
    )

    print("Inicialização do consumo da fila: lance_valido . . .")
    print("Inicialização do consumo da fila: leilao_vencedor . . .")

    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)