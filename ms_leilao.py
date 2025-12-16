import grpc
from concurrent import futures
import time
import json
import threading
from datetime import datetime
import pika
import leilao_pb2
import leilao_pb2_grpc

leiloes = []
# Exemplo inicial
leiloes.append({
    'id': 1, 'descricao': 'Atari', 'valor_inicial': 100.0,
    'inicio': datetime.now().timestamp() + 30,
    'fim': datetime.now().timestamp() + 90,
    'status': 'agendado'
})

class LeilaoService(leilao_pb2_grpc.SistemaLeilaoServicer):
    def CriarLeilao(self, request, context):
        novo = {
            'id': len(leiloes) + 1,
            'descricao': request.descricao,
            'valor_inicial': request.valor_inicial,
            'inicio': datetime.fromisoformat(request.inicio).timestamp(),
            'fim': datetime.fromisoformat(request.fim).timestamp(),
            'status': 'agendado'
        }
        leiloes.append(novo)
        return leilao_pb2.RespostaID(msg="Leilão criado", id=novo['id'])

    def ListarLeiloes(self, request, context):
        lista = leilao_pb2.ListaLeiloes()
        for l in leiloes:
            if l['status'] in ['ativo', 'agendado']:
                item = lista.leiloes.add()
                item.id = l['id']
                item.descricao = l['descricao']
                # Aqui retornamos o valor atualizado!
                item.valor_inicial = l['valor_inicial'] 
                item.status = l['status']
        return lista

# Para ouvir lances e atualizar valores
def rabbitmq_update_listener():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Ouve exchange de lances
    channel.exchange_declare(exchange='lances_fanout', exchange_type='fanout')
    result = channel.queue_declare(queue='', exclusive=True)
    q_name = result.method.queue
    channel.queue_bind(exchange='lances_fanout', queue=q_name)

    def callback(ch, method, properties, body):
        dados = json.loads(body)
        lid = int(dados['ID do leilão'])
        novo_valor = float(dados['valor do lance'])
        
        # Atualiza memória local
        for l in leiloes:
            if l['id'] == lid:
                l['valor_inicial'] = novo_valor # Atualiza o "preço atual"
                print(f"[MS Leilão] Preço do leilão {lid} atualizado para {novo_valor}")

    channel.basic_consume(queue=q_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

def time_checker():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='leiloes_fanout', exchange_type='fanout')
    channel.queue_declare(queue='leilao_finalizado')

    while True:
        agora = datetime.now().timestamp()
        for leilao in leiloes:
            if leilao['status'] == 'agendado' and agora >= leilao['inicio']:
                leilao['status'] = 'ativo'
                evento = {'ID do leilão': leilao['id'], 'descrição': leilao['descricao'], 'status': 'ativo'}
                channel.basic_publish(exchange='leiloes_fanout', routing_key='', body=json.dumps(evento))
                print(f"[MS Leilão] Leilão {leilao['id']} INICIADO.")

            if leilao['status'] == 'ativo' and agora >= leilao['fim']:
                leilao['status'] = 'encerrado'
                evento = {'ID do leilão': leilao['id']}
                channel.basic_publish(exchange='', routing_key='leilao_finalizado', body=json.dumps(evento))
                print(f"[MS Leilão] Leilão {leilao['id']} FINALIZADO.")
        time.sleep(1)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    leilao_pb2_grpc.add_SistemaLeilaoServicer_to_server(LeilaoService(), server)
    server.add_insecure_port('[::]:5001')
    print("MS Leilão (gRPC) rodando na porta 5001...")
    
    # Inicia Threads
    t1 = threading.Thread(target=time_checker, daemon=True)
    t2 = threading.Thread(target=rabbitmq_update_listener, daemon=True) # Nova thread
    t1.start()
    t2.start()
    
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()