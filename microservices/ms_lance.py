import pika as rabbit
import sys, os
import json
import base64
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA

leiloes_ativos = {}
    # ID
    # nome
    # descrição
    # data_inicio
    # data_fim
    # assinatura
    # status
    # lance // maior lance
    # ID_usuario // do maior lance

keys = {} # cache

def callback_leilao_inicializado(ch, method, properties, body: bytes):
    data = json.loads(body.decode('utf-8')) # bytes -> str -> dict
    # ID
    # nome
    # descrição
    # data_inicio
    # data_fim
    # status

    # Adicionei o leilao incializados aos leiloes ativos
    leiloes_ativos[data['ID']] = data
    print("********************************")
    print("Recebido Leilao Iniciado")
    print(f"ID = {data['ID']}")
    print("********************************")


def callback_lance_realizado(ch, method, properties, body: bytes):
    # ID_leilao
    # ID_usuario
    # lance
    # assinatura

    pack = json.loads(body.decode('utf-8').strip()) # bytes -> str -> dict
    print(f"Lance recebido: {body.decode('utf-8')}")
    # Verificação se o leilão existe
    if not pack['ID_leilao'] in leiloes_ativos:
        print("Lance Invalido: Leilao não existe!")
        return
    
    # Verificar a assinatura
    if not os.path.exists(f"./keys/{pack['ID_usuario']}"):
        print("Lance Invalido: Chave publica do usuario não existe.")
        return

    if not 'assinatura' in pack:
        print("Lance Invalido: Não encontrou a assinatura no pacote")
        return

    signature = base64.b64decode(pack['assinatura'].encode("utf-8"))

    del pack['assinatura']
    message = json.dumps(pack, sort_keys=True).encode('utf-8') # dict -> str -> bytes

    key = RSA.import_key(open(f"./keys/{pack['ID_usuario']}/public_key.der").read())
    h = SHA256.new(message)
    try:
        pkcs1_15.new(key).verify(h, signature)
        print("The signature is valid.")
    except (ValueError, TypeError):
        print("Lance Invalido: The signature is not valid.")
        return

    # Verificar se o lance é maior
    leilao = leiloes_ativos[pack['ID_leilao']]
    pack['lance'] = float(pack['lance'])
    if not 'lance' in leilao:
        leilao['lance'] = pack['lance']
        leilao['ID_usuario'] = pack['ID_usuario']
    else:
        if(pack['lance'] <= leilao['lance']):
            print("Lance Inválido: Lance menor ao maior lance em vigor.")
            return
        leilao['lance'] = pack['lance']
        leilao['ID_usuario'] = pack['ID_usuario']
    print("********************************")
    print("Lance Valido: ")
    print(f"Leilao: {leilao['nome']}")
    print(f"Usuario: {pack['ID_usuario']}")
    print(f"Lance: {pack['lance']}")
    print("********************************")
    # Lance é válido, portanto deve comunicar ms_notificacao
    ch.basic_publish(
        exchange='direct_lance',
        routing_key='lance_valido',
        body=message.decode('utf-8') # bytes -> str
    )

def callback_leilao_finalizado(ch, method, properties, body: bytes):
    # Só tem o ID do leilao
    # remover o leilao dos leiloes ativos
    message = {}
    leilao: dict = leiloes_ativos[body.decode('utf-8')]
    message['ID_leilao'] = body.decode('utf-8')
    message['ID_usuario'] = leilao['ID_usuario']
    message['lance'] = leilao['lance']
    message = json.dumps(message, sort_keys=True)
    print("********************************")
    print("Leilao Finalizado: ")
    print(f"Leilao: {leilao['ID']}")
    print(f"Leilao: {leilao['nome']}")
    if 'lance' in leilao:
        print(f"ID vencedor: {leilao['ID_usuario']}")
        print(f"Valor do Lance vencedor: {leilao['lance']}")
    

    del leiloes_ativos[body.decode('utf-8')]
    # enviar o vencedor do leilao
    ch.basic_publish(
        exchange='direct_lance',
        routing_key='leilao_vencedor',
        auto_ack=True
    )

def main():
    broker_IP = 'localhost'
    address = rabbit.ConnectionParameters(broker_IP)
    connection = rabbit.BlockingConnection(address)
    channel = connection.channel()

    # Declaração das Exchanges
    channel.exchange_declare(exchange='fanout_leilao', exchange_type='fanout')
    channel.exchange_declare(exchange='direct_leilao', exchange_type='direct')

    channel.exchange_declare(exchange='direct_lance', exchange_type='direct')

    # Declaração das Filas
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_declare(queue='lance_realizado', exclusive=False)
    channel.queue_declare(queue='leilao_finalizado', exclusive=False)

    # Vinculação das Filas
    channel.queue_bind(queue=queue_name, exchange='fanout_leilao')
    channel.queue_bind(queue='lance_realizado', exchange='direct_lance', routing_key='lance_realizado')
    channel.queue_bind(queue='leilao_finalizado', exchange='direct_leilao', routing_key='leilao_finalizado')

    # Quais filas serão consumidas
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback_leilao_inicializado,
        auto_ack=True
    )
    
    channel.basic_consume(
        queue='lance_realizado',
        on_message_callback=callback_lance_realizado,
        auto_ack=True
    )

    channel.basic_consume(
        queue='leilao_finalizado',
        on_message_callback=callback_leilao_finalizado,
        auto_ack=True
    )
    print("Inicialização do consumo da fila: leilao incializado . . .")
    print("Inicialização do consumo da fila: lance realizado . . .")
    print("Inicialização do consumo da fila: lance finalizado . . .")
    channel.start_consuming()

def createKeys():

    # Verificando se as pastas existem e, caso contrario, criada as pastas
    if not os.path.exists("./keys/backend"):
        os.makedirs("./keys/backend")
        print("Criado as pastas 'keys' e 'backends' para os microservices")

    if not os.path.exists("../client/keys/backend"):
        os.makedirs("../client/keys/backend")

    # Gerando as chaves publicas e privadas
    key = RSA.generate(2048)
    private_key = key.export_key()
    public_key = key.publickey().export_key()
    
    # Salvando as keys
    with open(f"./keys/backend/private_key.der", "wb") as file:
        file.write(private_key)

    with open(f"./keys/backend/public_key.der", "wb") as file:
        file.write(public_key)

    with open(f"../client/keys/backend/public_key.der", "wb") as file:
        file.write(public_key)

if __name__ == '__main__':
    try:
        createKeys()
        main()
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)