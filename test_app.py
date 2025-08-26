import pytest
import json
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi.testclient import TestClient
from app import app, notificacoes, QUEUE_NAME, RETRY_QUEUE, VALIDATION_QUEUE

client = TestClient(app)

sample_message = {
    "traceId": "1234-5678",
    "mensagemId": "abcd-efgh",
    "conteudoMensagem": "Mensagem de teste",
    "tipoNotificacao": "EMAIL",
    "status": "RECEBIDO"
}

@pytest.fixture(autouse=True)
def clear_notificacoes():
    notificacoes.clear()
    notificacoes[sample_message["traceId"]] = sample_message.copy()

@patch("app.pika.BlockingConnection")
@patch("app.random.randint")
@patch("app.asyncio.sleep", new_callable=AsyncMock)
def test_consume_message_one_success(mock_sleep, mock_randint, mock_pika):
    from app import channel, VALIDATION_QUEUE, notificacoes

    mock_conn = MagicMock()
    mock_channel = MagicMock()
    mock_pika.return_value = mock_conn
    mock_conn.channel.return_value = mock_channel

    # Forçar sucesso
    mock_randint.return_value = 50

    # Mock do basic_get retornando a mensagem
    sample_message = {
        "traceId": "1234-5678",
        "mensagemId": "abcd-efgh",
        "conteudoMensagem": "Mensagem de teste",
        "tipoNotificacao": "EMAIL",
        "status": "RECEBIDO"
    }
    mock_channel.basic_get.return_value = (MagicMock(), MagicMock(), json.dumps(sample_message).encode())
    mock_channel.basic_publish = MagicMock()

    # Substitui o channel global pelo mock
    channel = mock_channel

    from fastapi.testclient import TestClient
    from app import app
    client = TestClient(app)

    response = client.get("/api/consumer_one/")
    assert response.status_code == 200
    data = response.json()
    assert data["traceId"] == sample_message["traceId"]
    assert data["mensagemId"] == sample_message["mensagemId"]

    mock_channel.basic_publish.assert_called_with(
        exchange="",
        routing_key=VALIDATION_QUEUE,
        body=json.dumps(sample_message),
        properties=mock_channel.basic_publish.call_args[1]['properties']
    )


@patch("app.pika.BlockingConnection")
@patch("app.random.randint")
@patch("app.asyncio.sleep", new_callable=AsyncMock)  # CORRIGIDO
def test_consume_message_one_failure(mock_sleep, mock_randint, mock_pika):
    mock_conn = MagicMock()
    mock_channel = MagicMock()
    mock_pika.return_value = mock_conn
    mock_conn.channel.return_value = mock_channel

    mock_randint.return_value = 10  # Forçar falha

    mock_channel.basic_get.return_value = (MagicMock(), MagicMock(), json.dumps(sample_message).encode())
    mock_channel.basic_publish = MagicMock()

    response = client.get("/api/consumer_one/")
    assert response.status_code == 400
    data = response.json()
    assert data["traceId"] == sample_message["traceId"]
    assert data["status"] == "FALHA_PROCESSAMENTO_INICIAL"

    mock_channel.basic_publish.assert_called_with(
        exchange="",
        routing_key=RETRY_QUEUE,
        body=json.dumps(sample_message),
        properties=mock_channel.basic_publish.call_args[1]['properties']
    )
