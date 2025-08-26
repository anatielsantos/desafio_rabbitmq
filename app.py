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
QUEUE_NAME = "notification.queue.input.anatielsantos"
RETRY_QUEUE = "notification.queue.retry.anatielsantos"
VALIDATION_QUEUE = "notification.queue.validation.anatielsantos"
DEAD_QUEUE = "notification.queue.dlq.anatielsantos"

connection = None
channel = None

notifications: dict[str, dict] = {}

class NotificationPayload(BaseModel):
    message_id: UUID4 | None = None
    message_content: constr(min_length=1, max_length=1000)
    notification_type: constr(min_length=1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global connection, channel
    params = pika.URLParameters(AMQP_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_declare(queue=RETRY_QUEUE, durable=True)
    channel.queue_declare(queue=VALIDATION_QUEUE, durable=True)
    channel.queue_declare(queue=DEAD_QUEUE, durable=True)

    yield

    if connection and connection.is_open:
        connection.close()

app = FastAPI(lifespan=lifespan)

@app.post("/api/notificar/")
async def publish_message(payload: NotificationPayload):
    trace_id = str(uuid.uuid4())
    message_id = str(payload.message_id or uuid.uuid4())

    message = {
        "traceId": trace_id,
        "messageId": message_id,
        "messageContent": payload.message_content,
        "notificationType": payload.notification_type,
        "status": "RECEIVED"
    }

    notifications[trace_id] = message

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
            "messageId": message_id,
            "status": "Accepted for asynchronous processing"
        }
    )

@app.get("/api/notificar/{trace_id}")
async def get_notification(trace_id: str):
    notification = notifications.get(trace_id)
    if not notification:
        return {"error": "TraceId not found"}
    return notification

@app.get("/api/consumer_one/")
async def consume_message_one():
    global channel
    method_frame, header_frame, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=True)
    if not body:
        return {"status": "queue empty"}

    message = json.loads(body.decode())
    trace_id = message.get("traceId")

    failure_chance = random.randint(1, 100)
    if failure_chance <= 15:
        if trace_id in notifications:
            notifications[trace_id]["status"] = "INITIAL_PROCESSING_FAILURE"

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
                "status": "INITIAL_PROCESSING_FAILURE"
            }
        )
    else:
        await asyncio.sleep(random.uniform(1, 1.5))
        if trace_id in notifications:
            notifications[trace_id]["status"] = "INTERMEDIATE_PROCESSED"
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
    global channel
    method_frame, header_frame, body = channel.basic_get(queue=RETRY_QUEUE, auto_ack=True)
    if not body:
        return {"status": "queue empty"}

    await asyncio.sleep(3)

    message = json.loads(body.decode())
    trace_id = message.get("traceId")

    failure_chance = random.randint(1, 100)
    if failure_chance <= 20:
        if trace_id in notifications:
            notifications[trace_id]["status"] = "FINAL_REPROCESSING_FAILURE"

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
                "status": "FINAL_REPROCESSING_FAILURE"
            }
        )
    else:
        if trace_id in notifications:
            notifications[trace_id]["status"] = "REPROCESSED_SUCCESSFULLY"
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
    global channel
    method_frame, header_frame, body = channel.basic_get(queue=VALIDATION_QUEUE, auto_ack=True)
    if not body:
        return {"status": "queue empty"}

    await asyncio.sleep(3)

    message = json.loads(body.decode())
    trace_id = message.get("traceId")
    notification_type = message.get("notificationType", "").upper()

    failure_chance = random.randint(1, 100)
    if failure_chance <= 5:
        await asyncio.sleep(random.uniform(1, 1.5))
        if trace_id in notifications:
            notifications[trace_id]["status"] = "FINAL_SEND_FAILURE"
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
                "status": "INITIAL_PROCESSING_FAILURE"
            }
        )
    else:
        if trace_id in notifications:
            notifications[trace_id]["status"] = "SENT_SUCCESS"

        if notification_type == "EMAIL":
            send_simulation = {"message": f"[EMAIL] Sending email: {message['messageContent']}"}
        elif notification_type == "SMS":
            send_simulation = {"message": f"[SMS] Sending SMS: {message['messageContent']}"}
        elif notification_type == "PUSH":
            send_simulation = {"message": f"[PUSH] Sending push notification: {message['messageContent']}"}
        else:
            send_simulation = {"message": f"[UNKNOWN] Invalid type: {notification_type}"}

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=send_simulation
    )

@app.get("/api/consumer_four/")
async def consume_message_four():
    global channel
    method_frame, header_frame, body = channel.basic_get(queue=DEAD_QUEUE, auto_ack=True)
    if not body:
        return {"status": "queue empty"}

    message = json.loads(body.decode())

    return JSONResponse(
        status_code=200,
        content={
            "content": message,
            "message": "This message will no longer be processed"
        }
    )
