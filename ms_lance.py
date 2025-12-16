import grpc
from concurrent import futures
import json
import threading
import pika
import leilao_pb2
import leilao_pb2_grpc

leiloes_ativos = set()
maiores_lances = {} 

class LanceService(leilao_pb2_grpc.SistemaLeilaoServicer):
    def DarLance(self, request, context):
        lid = request.leilao_id
        uid = request.user_id
        val = request.valor

        if lid not in leiloes_ativos:
            return leilao_pb2.RespostaStatus(status="erro", msg="Leilão não está ativo")
        
        atual = maiores_lances.get(lid, {'valor': 0.0})['valor']
        # O valor deve ser estritamente maior
        # Se for o primeiro lance, atual é 0. Se valor for 100, ok.
        # Se o leilao começa com 100, o MS Leilao deve validar ou o lance inicial deve ser > valor_inicial
        # Aqui assumimos que a logica de negocio permite, focando na atualizacao
        if val <= atual:
            return leilao_pb2.RespostaStatus(status="erro", msg="Valor deve ser maior que o atual")

        maiores_lances[lid] = {'usuario': uid, 'valor': val}
        
        # Publicar em FANOUT 
        msg = {'ID do leilão': lid, 'ID do usuário': uid, 'valor do lance': val}
        conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        ch = conn.channel()
        
        # Declara exchange fanout para lances
        ch.exchange_declare(exchange='lances_fanout', exchange_type='fanout')
        
        # Publica para o exchange (não mais direto para fila)
        ch.basic_publish(exchange='lances_fanout', routing_key='', body=json.dumps(msg))
        conn.close()
        
        print(f"[MS Lance] Lance de {val} aceito para {lid}")
        return leilao_pb2.RespostaStatus(status="sucesso", msg="Lance registrado")

def rabbitmq_listener():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Setup para ouvir timer do leilão
    channel.exchange_declare(exchange='leiloes_fanout', exchange_type='fanout')
    q_name = channel.queue_declare(queue='', exclusive=True).method.queue
    channel.queue_bind(exchange='leiloes_fanout', queue=q_name)
    
    channel.queue_declare(queue='leilao_finalizado')
    channel.queue_declare(queue='leilao_vencedor')

    def callback_inicio(ch, method, properties, body):
        dados = json.loads(body)
        leiloes_ativos.add(dados['ID do leilão'])
        # Inicializa o valor no dicionário se não existir, para validação correta
        if dados['ID do leilão'] not in maiores_lances:
             maiores_lances[dados['ID do leilão']] = {'usuario': None, 'valor': 0.0}
        print(f"[MS Lance] Leilão {dados['ID do leilão']} ATIVO.")

    def callback_fim(ch, method, properties, body):
        dados = json.loads(body)
        lid = dados['ID do leilão']
        if lid in leiloes_ativos:
            leiloes_ativos.remove(lid)
            vencedor = maiores_lances.get(lid)
            if vencedor and vencedor['usuario']:
                msg = {
                    'ID do leilão': lid,
                    'ID do vencedor do leilão': vencedor['usuario'],
                    'valor negociado': vencedor['valor']
                }
                pub_conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                pc = pub_conn.channel()
                pc.queue_declare(queue='leilao_vencedor')
                pc.basic_publish(exchange='', routing_key='leilao_vencedor', body=json.dumps(msg))
                pub_conn.close()
                print(f"[MS Lance] Vencedor do {lid} publicado.")

    channel.basic_consume(queue=q_name, on_message_callback=callback_inicio, auto_ack=True)
    channel.basic_consume(queue='leilao_finalizado', on_message_callback=callback_fim, auto_ack=True)
    channel.start_consuming()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    leilao_pb2_grpc.add_SistemaLeilaoServicer_to_server(LanceService(), server)
    server.add_insecure_port('[::]:5002')
    print("MS Lance (gRPC) rodando na porta 5002...")
    t = threading.Thread(target=rabbitmq_listener, daemon=True)
    t.start()
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()