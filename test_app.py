import pytest
import json
from unittest.mock import patch, MagicMock
from fastapi import status
from app import publish_message, NotificacaoPayload, notificacoes, QUEUE_NAME

@pytest.mark.asyncio
async def test_publish_message():
    payload = NotificacaoPayload(
        conteudoMensagem="Teste de notificação",
        tipoNotificacao="EMAIL"
    )

    with patch("app.channel") as mock_channel:
        mock_channel.basic_publish = MagicMock()

        response = await publish_message(payload)

        content_bytes = response.body
        content_dict = json.loads(content_bytes)

        assert "traceId" in content_dict
        assert "mensagemId" in content_dict
        assert content_dict["status"] == "Recebido para processamento assíncrono"

        trace_id = content_dict["traceId"]
        assert trace_id in notificacoes
        assert notificacoes[trace_id]["conteudoMensagem"] == payload.conteudoMensagem
        assert notificacoes[trace_id]["tipoNotificacao"] == payload.tipoNotificacao

        mock_channel.basic_publish.assert_called_once()
        args, kwargs = mock_channel.basic_publish.call_args
        assert kwargs["routing_key"] == QUEUE_NAME
        body_dict = json.loads(kwargs["body"])
        assert body_dict["traceId"] == trace_id
        assert body_dict["mensagemId"] == content_dict["mensagemId"]
        assert body_dict["conteudoMensagem"] == payload.conteudoMensagem
        assert body_dict["tipoNotificacao"] == payload.tipoNotificacao
        assert body_dict["status"] == "RECEBIDO"
