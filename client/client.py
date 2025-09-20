import pika as rabbit
import sys, os
import threading
import datetime
import time
import json
import uuid
import base64
import queue
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

LEILAO_INICIALIZADO = 0
LEILAO_FINALIZADO = 2
LANCE_VALIDO = 3
LANCE_MENOR = 4

lock = threading.Lock()
startup_event = threading.Event()
package_queue = queue.Queue()


def handle_incoming_package(package: dict, leiloes_ativos: dict, leiloes_finalizados: dict, historico_lance: dict, lnotificacao: list, leiloes_escutados: list) -> None:
    if 'type' in package:
        if package['type'] == LEILAO_INICIALIZADO:
            leiloes_ativos[package['ID_leilao']] = package
        elif package['type'] == LEILAO_FINALIZADO:
            if package['ID_leilao'] in leiloes_ativos:
                leiloes_finalizados[package['ID_leilao']] = leiloes_ativos[package['ID_leilao']]
                del leiloes_ativos[package['ID_leilao']]
            leiloes_escutados.remove(package['ID_leilao'])
            if package['ID_leilao'] in leiloes_escutados:
                leiloes_escutados.remove(package['ID_leilao'])
        elif package['type'] == LANCE_VALIDO:
            leilao = leiloes_ativos[package['ID_leilao']]
            leilao['lance'] = package['lance']
            leilao['ID_usuario'] = package['ID_usuario']
        elif package['type'] == LANCE_MENOR:
            pass
    else:
        print("*" * 22)
        print("Pacote sem TIPO:")
        for value in package.values():
            print(str(value))

    lnotificacao.append(package)


def callback_listen_topic(ch, method, properties, body: bytes):
    data = json.loads(body.decode('utf-8'))
    package_queue.put(data)
    if data['type'] == LEILAO_FINALIZADO:
        ch.stop_consuming()  # para o loop


def listen_topic(ID: str):
    broker_IP = 'localhost'
    address = rabbit.ConnectionParameters(broker_IP)
    connection = rabbit.BlockingConnection(address)
    channel = connection.channel()

    # Declarar as Exchanges
    channel.exchange_declare(exchange='topic_notificacao', exchange_type='topic')
    topic = f"leilao.{ID}"

    # Declarar as Filas
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    # Vincular as filas
    channel.queue_bind(queue=queue_name, exchange='topic_notificacao', routing_key=topic)

    # Filas que serão consumidas
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback_listen_topic,
        auto_ack=True
    )

    channel.start_consuming()


def lance(client: dict, leiloes_escutados: list):
    # ID_leilao
    # ID_usuario
    # lance
    # assinatura
    
    broker_IP = 'localhost'
    address = rabbit.ConnectionParameters(broker_IP)
    connection = rabbit.BlockingConnection(address)
    channel = connection.channel()

    channel.exchange_declare(exchange='direct_lance', exchange_type='direct')

    package = {}
    package['ID_leilao'] = str(input("Digite o ID do Leilao: "))
    package['ID_usuario'] = client['ID']
    package['lance'] = str(input("Digite o valor do lance: "))

    msg = json.dumps(package, sort_keys=True).encode('utf-8')
    key = RSA.import_key(client['private_key'])
    h = SHA256.new(msg)
    signature = pkcs1_15.new(key).sign(h)

    package['assinatura'] = base64.b64encode(signature).decode('utf-8')

    message = json.dumps(package, sort_keys=True)

    channel.basic_publish(
        exchange='direct_lance',
        routing_key='lance_realizado',
        body=message
    )
    if package['ID_leilao'] not in leiloes_escutados:
        # Thread exclusiva para escutar o topico do leilao
        threading.Thread(target=listen_topic, args=(package['ID_leilao'],), daemon=True, name=package['ID_leilao']).start()
        leiloes_escutados.append(package['ID_leilao'])

    connection.close()


def getCustomerData() -> dict:
    client = {}
    client['name'] = str(input("Digite o nome do cliente: "))
    # TODO Verificar se o nome é valido

    client['ID'] = str(uuid.uuid4())

    return client


def createdKeys(ID: str) -> bytes:

    # Criando as pastas para colocar as chaves
    folder_name = 'keys'
    os.makedirs(os.path.join(folder_name, ID), exist_ok=True)
    os.makedirs(os.path.join("..", "microservices", "keys", ID), exist_ok=True)

    # Gerando as chaves publicas e privadas
    key = RSA.generate(2048)
    private_key = key.export_key()
    public_key = key.publickey().export_key()

    with open(f"./keys/{ID}/public_key.der", "wb") as f:
        f.write(public_key)

    with open(f"../microservices/keys/{ID}/public_key.der", "wb") as file:
        file.write(public_key)

    return private_key


def callback_leilao_inicializado(ch, method, properties, body: bytes):
    msg = json.loads(body.decode('utf-8')) # bytes -> str -> dict
    package_queue.put(msg)


def main(client: dict):
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
    startup_event.set() # avisa que o menu já pode continuar
    channel.start_consuming()


def criar_leilao():

    broker_IP = 'localhost'
    address = rabbit.ConnectionParameters(broker_IP)
    connection = rabbit.BlockingConnection(address)
    channel = connection.channel()

    # declarar as Exchange
    channel.exchange_declare(exchange='direct_leilao', exchange_type='direct')

    # Construção do pacote para requisição
    package = {}
    data_inicio = datetime.datetime.now()
    package['nome'] = str(input("Digite o nome do leilão: "))
    package['descricao'] = str(input("Digite a descrição do leilão: "))
    package['data_inicio'] = data_inicio.strftime("%d/%m/%Y %H:%M:%S")
    minute = int(input("Digite o tempo do leilao em minutos: "))

    data_inicio = datetime.datetime.now()
    data_fim = data_inicio + datetime.timedelta(minutes=minute)
    package['data_fim'] = data_fim.strftime("%d/%m/%Y %H:%M:%S")
    
    message = json.dumps(package, sort_keys=True)

    channel.basic_publish(
        exchange='direct_leilao',
        routing_key='solicitacao_leilao',
        body=message
    )

    connection.close()


def lista_leiloes(leiloes_ativos: dict, leiloes_finalizados: dict):
    for leilao in leiloes_finalizados.values():
        print("*" * 22)
        print(f"ID: {leilao['ID_leilao']}")
        print(f"Nome: {leilao['nome']}")
        print(f"Descrição: {leilao['descricao']}")
        print(f"Data de Inicio: {leilao['data_inicio']}")
        print(f"Data de Fim: {leilao['data_fim']}")
        print(f"Status: {leilao['status']}")

    for leilao in leiloes_ativos.values():
        print("*" * 22)
        print(f"ID: {leilao['ID_leilao']}")
        print(f"Nome: {leilao['nome']}")
        print(f"Descrição: {leilao['descricao']}")
        print(f"Data de Inicio: {leilao['data_inicio']}")
        print(f"Data de Fim: {leilao['data_fim']}")
        print(f"Status: {leilao['status']}")


def lista_leiloes_ativos(leiloes_ativos: dict):
    print(f"Quantidade de leiloes ativos: {len(leiloes_ativos)}")
    for leilao in leiloes_ativos.values():
        print("*" * 22)
        print(f"ID: {leilao['ID_leilao']}")
        print(f"Nome: {leilao['nome']}")
        print(f"Descrição: {leilao['descricao']}")
        print(f"Data de Inicio: {leilao['data_inicio']}")
        print(f"Data de Fim: {leilao['data_fim']}")
        print(f"Status: {leilao['status']}")


def lista_leiloes_finalizados(leiloes_finalizados: dict):
    print(f"Quantidade de leiloes finalizados: {len(leiloes_finalizados)}")
    for leilao in leiloes_finalizados.values():
        print("*" * 22)
        print(f"ID: {leilao['ID_leilao']}")
        print(f"Nome: {leilao['nome']}")
        print(f"Descrição: {leilao['descricao']}")
        print(f"Data de Inicio: {leilao['data_inicio']}")
        print(f"Data de Fim: {leilao['data_fim']}")
        print(f"Status: {leilao['status']}")


def notificacao(lnotificacao: list):
    for package in lnotificacao:
        package: dict
        for key, value in package.items():
            print(f"{key}: {value}")
        del package
    lnotificacao.clear()

def menu(client: dict):
    # dict with keys being the IDs
    leiloes_ativos = {}
    leiloes_finalizados = {}
    historico_lance = {}
    lnotificacao = []
    leiloes_escutados = []
    
    startup_event.wait()

    while(True):
        while True:
            try:
                package = package_queue.get_nowait()
                handle_incoming_package(package, leiloes_ativos, leiloes_finalizados, historico_lance, lnotificacao, leiloes_escutados)
            except queue.Empty:
                break
        with lock:
            print("*" * (22 + len("Comandos")))
            print("*" * 10 ,"Comandos", "*" * 10)
            print("criar_leilao")
            print("lista_leiloes")
            print("lista_leiloes_ativos")
            print("lista_leiloes_finalizados")
            print("lista_historico_leilao")
            print(f"notificacao: {len(lnotificacao)} lances recebidos ou leiloes iniciados ou finalizados sem ter sido visto")
            print("lance")
            command = str(input("Digite o comando: "))

        if command == 'criar_leilao':
            criar_leilao()
        elif command == 'lista_leiloes':
            lista_leiloes(leiloes_ativos, leiloes_finalizados)
        elif command == 'lista_leiloes_ativos':
            lista_leiloes_ativos(leiloes_ativos)
        elif command == 'lista_leiloes_finalizados':
            lista_leiloes_finalizados(leiloes_finalizados)
        elif command == 'lista_historico_leilao':
            pass
        elif command == 'notificacao':
            notificacao(lnotificacao)
        elif command == 'lance':
            lance(client, leiloes_escutados)
        else:
            print("Commando desconhecido", end=" ")
            time.sleep(0.3)
            print(". ", end="")
            time.sleep(0.3)
            print(". ", end="")
            time.sleep(0.3)
            print('.')
            time.sleep(0.3)


if __name__ == '__main__':
    try:
        # Obtendo as informações iniciais do cliente
        client = getCustomerData()

        # Criação das chaves publicas e privadas
        client['private_key'] = createdKeys(client['ID'])

        t1 = threading.Thread(target=main, args=(client,), daemon=True, name="Listen")
        t2 = threading.Thread(target=menu, args=(client,), daemon=True, name="menu")
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)