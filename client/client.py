import pika as rabbit
import sys, os
import threading
import datetime
import time
import json
import uuid
import base64
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

def lance(client: dict):
    # ID_leilao
    # ID_usuario
    # lance
    # assinatura
    time.sleep(10)
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
    
    key = RSA.import_key(open(f"./keys/{client['ID']}/private_key.der").read())
    h = SHA256.new(msg)
    signature = pkcs1_15.new(key).sign(h)

    package['assinatura'] = base64.b64encode(signature).decode('utf-8')

    message = json.dumps(package, sort_keys=True)

    channel.basic_publish(
        exchange='direct_lance',
        routing_key='lance_realizado',
        body=message
    )
    print(f"Message: {message}")
    connection.close()


def getCustomerData() -> dict:
    client = {}
    client['name'] = str(input("Digite o nome do cliente: "))
    # TODO Verificar se o nome é valido

    client['ID'] = str(uuid.uuid4())

    return client

def createdKeys(ID: str):

    # Criando as pastas para colocar as chaves
    folder_name = 'keys'
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print("Criado a pasta keys para o client")
    
    if not os.path.exists(folder_name + f"/{ID}"):
        os.makedirs(folder_name + f"/{ID}")
        print(f"Criado a pasta keys/{ID} para o client")

    if not os.path.exists(f"../microservices/{folder_name}"):
        os.makedirs(f"../microservices/{folder_name}")
        print(f"Criado a pasta keys no microservices")

    if not os.path.exists(f"../microservices/{folder_name}/{ID}"):
        os.makedirs(f"../microservices/{folder_name}/{ID}")
        print(f"Criado a pasta {ID} no microservices/keys")

    # Gerando as chaves publicas e privadas
    key = RSA.generate(2048)
    private_key = key.export_key()
    public_key = key.publickey().export_key()

    with open(f"./keys/{ID}/private_key.der", "wb") as file:
        file.write(private_key)

    #with open(f"../microservices/keys/{ID}/private_key.der", "wb") as file:
    #    file.write(private_key)

    with open(f"./keys/{ID}/public_key.der", "wb") as f:
        f.write(public_key)

    with open(f"../microservices/keys/{ID}/public_key.der", "wb") as file:
        file.write(public_key)

def envio_leilao(client: dict):
    time.sleep(3)
    broker_IP = 'localhost'
    address = rabbit.ConnectionParameters(broker_IP)
    connection = rabbit.BlockingConnection(address)
    channel = connection.channel()

    # Construção do pacote para requisição
    package = {}
    data_inicio = datetime.datetime.now()
    data_fim = data_inicio + datetime.timedelta(minutes=1)
    package['nome'] = f"Leilao do {client['name']}"
    package['descricao'] = 'Leilao do meu coração'
    package['data_inicio'] = data_inicio.strftime("%d/%m/%Y %H:%M:%S")
    package['data_fim'] = data_fim.strftime("%d/%m/%Y %H:%M:%S")
    """
    request = json.dumps(package).encode('utf-8')
    
    key = RSA.import_key(open('./keys/{client['ID']}/private_key.der').read())
    h = SHA256.new(request)
    signature: bytes = pkcs1_15.new(key).sign(h)
    package['assinatura'] = signature.hex()
    """
    
    message = json.dumps(package, sort_keys=True)
    # declarar as Exchange
    channel.exchange_declare(exchange='direct_leilao', exchange_type='direct')

    channel.basic_publish(
        exchange='direct_leilao',
        routing_key='solicitacao_leilao',
        body=message
    )
    connection.close()


def callback_leilao_inicializado(ch, method, properties, body: bytes):
    msg = json.loads(body.decode('utf-8'))
    for key, value in msg.items():
        print(f"{key}: {value}")

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
    channel.start_consuming()

if __name__ == '__main__':
    try:
        # Obtendo as informações iniciais do cliente
        client = getCustomerData()

        # Criação das chaves publicas e privadas
        createdKeys(client['ID'])

        t1 = threading.Thread(target=main, args=(client,))
        t2 = threading.Thread(target=envio_leilao, args=(client,))
        t3 = threading.Thread(target=lance, args=(client, ))
        t1.start()
        t2.start()
        t3.start()
        t1.join()
        t2.join()
        t3.join()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)