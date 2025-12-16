import grpc
from concurrent import futures
import pika
import json
import threading
import leilao_pb2
import leilao_pb2_grpc

# Configuração gRPC para chamar o Banco
channel_banco = grpc.insecure_channel('localhost:5004')
stub_banco = leilao_pb2_grpc.BancoExternoStub(channel_banco)

# --- Servidor gRPC (Recebe o "Webhook" do Banco) ---
class CallbackService(leilao_pb2_grpc.NotificacaoPagamentoServicer):
    def ReceberStatus(self, request, context):
        print(f"[MS Pagamento] Callback recebido: Leilão {request.leilao_id} - {request.status}")
        
        # Publica evento status_pagamento para o Gateway
        evento_status = {
            "ID do leilão": request.leilao_id,
            "ID do usuário": request.cliente,
            "status_final": request.status
        }
        
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            channel = connection.channel()
            channel.queue_declare(queue='status_pagamento')
            channel.basic_publish(exchange='', routing_key='status_pagamento', body=json.dumps(evento_status))
            connection.close()
        except Exception as e:
            print(f"Erro RabbitMQ: {e}")
            
        return leilao_pb2.Vazio()

# --- Consumidor RabbitMQ (Inicia o fluxo) ---
def processar_vencedor(ch, method, properties, body):
    """Consome evento de vencedor e chama sistema externo via gRPC"""
    dados = json.loads(body)
    leilao_id = dados['ID do leilão']
    vencedor = dados['ID do vencedor do leilão']
    valor = float(dados['valor negociado'])
    
    print(f"[MS Pagamento] Processando vencedor: Leilão {leilao_id}")

    # Chama Sistema Externo via gRPC
    try:
        req = leilao_pb2.DadosPagamento(valor=valor, cliente=vencedor, leilao_id=leilao_id)
        resp = stub_banco.SolicitarPagamento(req)
        
        link = resp.link_pagamento
        print(f"[MS Pagamento] Link gerado: {link}")
        
        # Publica evento link_pagamento para o Gateway
        evento_link = {
            "ID do leilão": leilao_id,
            "ID do usuário": vencedor,
            "link": link,
            "msg": "Pagamento pendente"
        }
        
        conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        c = conn.channel()
        c.queue_declare(queue='link_pagamento')
        c.basic_publish(exchange='', routing_key='link_pagamento', body=json.dumps(evento_link))
        conn.close()
            
    except Exception as e:
        print(f"Erro ao chamar gRPC externo: {e}")

def rabbitmq_listener():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='leilao_vencedor')
    channel.basic_consume(queue='leilao_vencedor', on_message_callback=processar_vencedor, auto_ack=True)
    print("MS Pagamento ouvindo 'leilao_vencedor'...")
    channel.start_consuming()

def serve():
    # Inicia servidor gRPC para ouvir o callback na porta 5003
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    leilao_pb2_grpc.add_NotificacaoPagamentoServicer_to_server(CallbackService(), server)
    server.add_insecure_port('[::]:5003')
    print("MS Pagamento (gRPC Server) rodando na porta 5003...")
    
    # Thread do RabbitMQ
    t = threading.Thread(target=rabbitmq_listener, daemon=True)
    t.start()
    
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()