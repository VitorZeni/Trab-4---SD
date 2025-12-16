import grpc
from concurrent import futures
import time
import threading
import uuid
import leilao_pb2
import leilao_pb2_grpc

# Simulação de processamento bancário
def processar_fake(request_data, stub_callback):
    """Simula delay e chama o callback gRPC do MS Pagamento"""
    time.sleep(5) # Delay do banco
    
    print(f"[Sistema Externo] Processamento concluído. Enviando callback gRPC...")
    
    # Prepara dados do callback
    resultado = leilao_pb2.ResultadoPagamento(
        transacao_id=request_data['tx_id'],
        status="aprovado",
        leilao_id=request_data['leilao_id'],
        cliente=request_data['cliente'],
        valor=request_data['valor']
    )
    
    try:
        # Chama o MS Pagamento via gRPC (Substitui o POST do Webhook)
        stub_callback.ReceberStatus(resultado)
        print(f"[Sistema Externo] Callback enviado com sucesso.")
    except Exception as e:
        print(f"[Sistema Externo] Erro ao enviar callback: {e}")

class BancoService(leilao_pb2_grpc.BancoExternoServicer):
    def SolicitarPagamento(self, request, context):
        tx_id = str(uuid.uuid4())
        link_fake = f"https://pay.fake.com/{tx_id}"
        
        print(f"[Sistema Externo] Pagamento solicitado: {request.valor} (User: {request.cliente})")
        
        # Dados para passar para a thread
        data = {
            'tx_id': tx_id,
            'leilao_id': request.leilao_id,
            'cliente': request.cliente,
            'valor': request.valor
        }
        
        # Cria canal para chamar o MS Pagamento de volta
        channel = grpc.insecure_channel('localhost:5003')
        stub_callback = leilao_pb2_grpc.NotificacaoPagamentoStub(channel)
        
        t = threading.Thread(target=processar_fake, args=(data, stub_callback))
        t.start()
        
        return leilao_pb2.LinkPagamento(transacao_id=tx_id, link_pagamento=link_fake)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    leilao_pb2_grpc.add_BancoExternoServicer_to_server(BancoService(), server)
    server.add_insecure_port('[::]:5004')
    print("Sistema Externo (gRPC) rodando na porta 5004...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()