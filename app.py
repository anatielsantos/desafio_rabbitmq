import asyncio
from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, UUID4, constr
import pika
import uuid
import json
import random
from contextlib import asynccontextmanager

AMQP_URL = "amqp://bjnuffmq:gj-YQIiEXyfxQxjsZtiYDKeXIT8ppUq7@jaragua-01.lmq.cloudamqp.com/bjnuffmq"
QUEUE_NAME = "fila.notificacao.entrada.anatielsantos"
RETRY_QUEUE = "fila.notificacao.retry.anatielsantos"
VALIDATION_QUEUE = "fila.notificacao.validacao.anatielsantos"
DEAD_QUEUE = "fila.notificacao.dlq.anatielsantos"

connection = None
channel = None

notificacoes: dict[str, dict] = {}
print(notificacoes)


class NotificacaoPayload(BaseModel):
    mensagemId: UUID4 | None = None
    conteudoMensagem: constr(min_length=1, max_length=1000)
    tipoNotificacao: constr(min_length=1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gerencia ciclo de vida da aplicação
    """

    global connection, channel
    params = pika.URLParameters(AMQP_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # entrada
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    # retry
    channel.queue_declare(queue=RETRY_QUEUE, durable=True)
    # validacao
    channel.queue_declare(queue=VALIDATION_QUEUE, durable=True)
    # dlq
    channel.queue_declare(queue=DEAD_QUEUE, durable=True)

    yield

    if connection and connection.is_open:
        connection.close()


app = FastAPI(lifespan=lifespan)


@app.post("/api/notificar/")
async def publish_message(payload: NotificacaoPayload):
    """
    Publica uma mensagem no RabbitMQ e gera e persiste um traceId em memória
    """

    trace_id = str(uuid.uuid4())
    mensagem_id = str(payload.mensagemId or uuid.uuid4())

    message = {
        "traceId": trace_id,
        "mensagemId": mensagem_id,
        "conteudoMensagem": payload.conteudoMensagem,
        "tipoNotificacao": payload.tipoNotificacao,
        "status": "RECEBIDO"
    }

    notificacoes[trace_id] = message

    global channel

    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)
    )

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "traceId": trace_id,
            "mensagemId": mensagem_id,
            "status": "Recebido para processamento assíncrono"
        }
    )


@app.get("/api/notificar/{trace_id}")
async def consultar_notificacao(trace_id: str):
    """
    Consulta uma notificação já registrada pelo traceId
    """

    notificacao = notificacoes.get(trace_id)
    if not notificacao:
        return {"error": "TraceId não encontrado"}
    return notificacao


@app.get("/api/consumer_one/")
async def consume_message_one():
    """
    Consome uma mensagem da fila
    """

    global channel

    method_frame, header_frame, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=True)
    if not body:
        return {"status": "fila vazia"}

    message = json.loads(body.decode())
    trace_id = message.get("traceId")

    falha_chance = random.randint(1, 100)
    if falha_chance <= 15:
        if trace_id in notificacoes:
            notificacoes[trace_id]["status"] = "FALHA_PROCESSAMENTO_INICIAL"

        channel.basic_publish(
            exchange="",
            routing_key=RETRY_QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        return JSONResponse(
            status_code=400,
            content={
                "traceId": trace_id,
                "status": "FALHA_PROCESSAMENTO_INICIAL"
            }
        )
    else:
        await asyncio.sleep(random.uniform(1, 1.5))

        if trace_id in notificacoes:
            notificacoes[trace_id]["status"] = "PROCESSADO_INTERMEDIARIO"

            channel.basic_publish(
                exchange="",
                routing_key=VALIDATION_QUEUE,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)
            )

    message.pop("status")

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content=message
    )

@app.get("/api/consumer_two/")
async def consume_message_two():
    """
    Consome uma mensagem da fila
    """

    global channel

    method_frame, header_frame, body = channel.basic_get(queue=RETRY_QUEUE, auto_ack=True)
    if not body:
        return {"status": "fila vazia"}
    
    await asyncio.sleep(3)

    message = json.loads(body.decode())
    trace_id = message.get("traceId")

    falha_chance = random.randint(1, 100)
    if falha_chance <= 20:
        if trace_id in notificacoes:
            notificacoes[trace_id]["status"] = "FALHA_FINAL_REPROCESSAMENTO"

        channel.basic_publish(
            exchange="",
            routing_key=DEAD_QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        return JSONResponse(
            status_code=400,
            content={
                "traceId": trace_id,
                "status": "FALHA_FINAL_REPROCESSAMENTO"
            }
        )
    else:
        if trace_id in notificacoes:
            notificacoes[trace_id]["status"] = "REPROCESSADO_COM_SUCESSO"

            channel.basic_publish(
                exchange="",
                routing_key=VALIDATION_QUEUE,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)
            )

    message.pop("status")

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content=message
    )

@app.get("/api/consumer_three/")
async def consume_message_three():
    """
    Consome uma mensagem da fila
    """

    global channel

    method_frame, header_frame, body = channel.basic_get(queue=VALIDATION_QUEUE, auto_ack=True)
    if not body:
        return {"status": "fila vazia"}
    
    await asyncio.sleep(3)

    message = json.loads(body.decode())
    trace_id = message.get("traceId")
    tipo = message.get("tipoNotificacao", "").upper()

    falha_chance = random.randint(1, 100)
    if falha_chance <= 5:
        await asyncio.sleep(random.uniform(1, 1.5))

        if trace_id in notificacoes:
            notificacoes[trace_id]["status"] = "FALHA_ENVIO_FINAL"

            channel.basic_publish(
                exchange="",
                routing_key=DEAD_QUEUE,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)
            )

        return JSONResponse(
            status_code=400,
            content={
                "traceId": trace_id,
                "status": "FALHA_PROCESSAMENTO_INICIAL"
            }
        )
    else:
        if trace_id in notificacoes:
            notificacoes[trace_id]["status"] = "ENVIADO_SUCESSO"

        if tipo == "EMAIL":
            real_send_simulation = {"message": f"[EMAIL] Enviando email: {message['conteudoMensagem']}"}
        elif tipo == "SMS":
            real_send_simulation = {"message": f"[SMS] Enviando SMS: {message['conteudoMensagem']}"}
        elif tipo == "PUSH":
            real_send_simulation = {"message": f"[PUSH] Enviando notificação push: {message['conteudoMensagem']}"}
        else:
            real_send_simulation = {"message": f"[DESCONHECIDO] Tipo inválido: {tipo}"}

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=real_send_simulation
    )

@app.get("/api/consumer_four/")
async def consume_message_four():
    """
    Consome uma mensagem da fila
    """

    global channel

    method_frame, header_frame, body = channel.basic_get(queue=DEAD_QUEUE, auto_ack=True)
    if not body:
        return {"status": "fila vazia"}
    
    message = json.loads(body.decode())

    return JSONResponse(
        status_code=200,
        content={
            "content": message,
            "message": "Esta mensagem não será mais processada"
        }
    )
