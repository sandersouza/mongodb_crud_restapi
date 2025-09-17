# MongoDB CRUD REST API

Esta API utiliza FastAPI para expor operações de CRUD sobre uma coleção de séries temporais no MongoDB.

## Requisitos

- Python 3.11+
- MongoDB 6+

## Configuração

1. Copie o arquivo `.env.example` para `.env` e ajuste os valores conforme o seu ambiente.
2. Crie um ambiente virtual e instale as dependências:

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Execução

```bash
uvicorn app.main:app --reload
```

O parâmetro `--reload` garante que alterações no código sejam refletidas automaticamente durante o desenvolvimento.

### Exemplo de escrita rápida via `curl`

```bash
curl -X POST http://localhost:8000/api/records \
  -H "Content-Type: application/json" \
  -d '{
        "source": "station-01",
        "payload": {"temperature": 26.7, "humidity": 0.42},
        "metadata": {"city": "São Paulo"}
      }'
```

### Exemplo de busca

```bash
curl "http://localhost:8000/api/records/search?field=payload.temperature&value=26.7&latest=true"
```

## Endpoints principais

- `GET /healthz` — Verifica se o serviço está disponível.
- `POST /api/records` — Insere um novo registro de série temporal.
- `GET /api/records/{record_id}` — Recupera um registro específico.
- `GET /api/records/search` — Pesquisa registros por campo e janela temporal.
- `GET /api/records` — Lista registros com paginação.
- `PUT /api/records/{record_id}` — Atualiza um registro existente.
- `DELETE /api/records/{record_id}` — Remove um registro.

Consulte a documentação automática em `http://localhost:8000/docs` para mais detalhes.

## Docker

Um Dockerfile está disponível para facilitar a conteinerização. Para construir a imagem de produção:

```bash
docker build -t mongodb-crud-api .
```

Em seguida execute:

```bash
docker run --env-file .env -p 8000:8000 mongodb-crud-api
```

Certifique-se de disponibilizar o MongoDB acessível para o container (por exemplo, via rede Docker).
