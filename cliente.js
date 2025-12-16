const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const readline = require('readline-sync');

// Carrega o proto dinamicamente
const packageDefinition = protoLoader.loadSync('leilao.proto', {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
});
const proto = grpc.loadPackageDefinition(packageDefinition).leilao;

// Cliente conecta ao Gateway (porta 5000)
const client = new proto.SistemaLeilao('localhost:5000', grpc.credentials.createInsecure());
const CLIENT_ID = "ClienteNodeJS";

function iniciarStream() {
    console.log("--- Iniciando Stream gRPC ---");
    const stream = client.AcompanharLeiloes({ client_id: CLIENT_ID });

    stream.on('data', (evento) => {
        const dados = JSON.parse(evento.payload_extra);
        const tipo = evento.tipo_evento;

        if (tipo === 'lance_validado') {
            console.log(`\n[NOVO LANCE] Leilão ${evento.leilao_id}: R$ ${dados['valor do lance']} (User: ${evento.user_id})`);
        } else if (tipo === 'leilao_vencedor') {
            console.log(`\n[FIM LEILÃO] Vencedor: ${evento.user_id} | Valor: ${dados['valor negociado']}`);
        } else if (tipo === 'link_pagamento') {
            console.log(`\n[!!!] PAGAMENTO: ${dados.link}`);
        } else if (tipo === 'status_pagamento') {
            console.log(`\n[$$$] STATUS: ${dados.status_final}`);
        }
    });

    stream.on('end', () => console.log("Stream encerrado pelo servidor."));
    stream.on('error', (e) => console.log("Erro no Stream:", e.message));
}

function listarLeiloes() {
    client.ListarLeiloes({}, (err, response) => {
        if (err) console.error(err);
        else {
            console.log("\n--- Leilões ---");
            response.leiloes.forEach(l => {
                console.log(`ID: ${l.id} | ${l.descricao} | R$ ${l.valor_inicial} | ${l.status}`);
            });
        }
    });
}

function darLance() {
    const lid = readline.questionInt("ID do Leilão: ");
    const val = readline.questionFloat("Valor: ");
    
    const lance = {
        leilao_id: lid,
        user_id: CLIENT_ID,
        valor: val
    };

    client.DarLance(lance, (err, response) => {
        if (err) console.log("Erro:", err.message);
        else console.log("Resp:", response.msg);
    });
}

function menu() {
    iniciarStream(); // Inicia escuta em background

    // Loop simples para o menu não bloquear totalmente a thread principal do Node (que lida com eventos)
    // Nota: readline-sync bloqueia, então as notificações aparecem "misturadas" ou após o enter.
    // Em produção, usaria uma UI não bloqueante, mas para teste serve.
    const loop = () => {
        console.log("\n1. Listar | 2. Dar Lance | 0. Sair");
        const opt = readline.question("Opcao: ");
        
        if (opt === '1') {
            listarLeiloes();
            setTimeout(loop, 1000); // Pequeno delay para resposta chegar
        } else if (opt === '2') {
            darLance();
            setTimeout(loop, 1000);
        } else if (opt === '0') {
            process.exit(0);
        } else {
            loop();
        }
    };
    loop();
}

menu();