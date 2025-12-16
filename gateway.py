import grpc
from concurrent import futures
import threading
import queue
import json
import pika
import leilao_pb2
import leilao_pb2_grpc

# Filas de mensagens para clientes conectados via gRPC stream
# {client_id: queue.Queue}
connected_clients = {}
lock = threading.Lock()

# Conexões gRPC com os microsserviços
channel_leilao = grpc.insecure_channel('localhost:5001')
stub_leilao = leilao_pb2_grpc.SistemaLeilaoStub(channel_leilao)

channel_lance = grpc.insecure_channel('localhost:5002')
stub_lance = leilao_pb2_grpc.SistemaLeilaoStub(channel_lance)

class GatewayService(leilao_pb2_grpc.SistemaLeilaoServicer):
    
    def CriarLeilao(self, request, context):
        # Repassa para MS Leilão via gRPC
        return stub_leilao.CriarLeilao(request)

    def ListarLeiloes(self, request, context):
        # Repassa para MS Leilão via gRPC
        return stub_leilao.ListarLeiloes(request)

    def DarLance(self, request, context):
        # Repassa para MS Lance via gRPC
        return stub_lance.DarLance(request)

    def AcompanharLeiloes(self, request, context):
        client_id = request.client_id
        q = queue.Queue()
        
        with lock:
            connected_clients[client_id] = q
        print(f"[Gateway] Cliente {client_id} conectado no stream.")

        try:
            while True:
                # Bloqueia até chegar evento do RabbitMQ
                evento = q.get()
                yield evento
        except Exception as e:
            print(f"[Gateway] Cliente {client_id} desconectou.")
        finally:
            with lock:
                if client_id in connected_clients:
                    del connected_clients[client_id]

def rabbitmq_consumer():
    """Consome RabbitMQ e joga nas filas internas dos clientes gRPC"""
    conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    ch = conn.channel()
    
    # Binding do Fanout de Lances
    ch.exchange_declare(exchange='lances_fanout', exchange_type='fanout')
    
    # A fila 'lance_validado' agora recebe mensagens do exchange 'lances_fanout'
    ch.queue_declare(queue='lance_validado') 
    ch.queue_bind(exchange='lances_fanout', queue='lance_validado')
    
    # Outras filas (diretas)
    ch.queue_declare(queue='leilao_vencedor')
    ch.queue_declare(queue='link_pagamento')
    ch.queue_declare(queue='status_pagamento')
    
    def callback(ch, method, properties, body):
        dados = json.loads(body)
        
        # Se vier do Fanout, o routing_key pode vir vazio, então forçamos o tópico
        topic = method.routing_key
        if not topic and 'valor do lance' in dados:
            topic = 'lance_validado'
            
        lid = int(dados.get('ID do leilão', 0))
        uid = str(dados.get('ID do usuário', ''))

        evento_proto = leilao_pb2.Evento(
            tipo_evento=topic,
            leilao_id=lid,
            user_id=uid,
            payload_extra=json.dumps(dados)
        )

        with lock:
            for cid, q in connected_clients.items():
                if topic == 'link_pagamento' and cid != uid:
                    continue
                q.put(evento_proto)

    # Consome
    ch.basic_consume(queue='lance_validado', on_message_callback=callback, auto_ack=True)
    ch.basic_consume(queue='leilao_vencedor', on_message_callback=callback, auto_ack=True)
    ch.basic_consume(queue='link_pagamento', on_message_callback=callback, auto_ack=True)
    ch.basic_consume(queue='status_pagamento', on_message_callback=callback, auto_ack=True)

    print("Gateway ouvindo RabbitMQ...")
    ch.start_consuming()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    leilao_pb2_grpc.add_SistemaLeilaoServicer_to_server(GatewayService(), server)
    server.add_insecure_port('[::]:5000')
    print("API Gateway (gRPC) rodando na porta 5000...")
    
    t = threading.Thread(target=rabbitmq_consumer, daemon=True)
    t.start()
    
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()