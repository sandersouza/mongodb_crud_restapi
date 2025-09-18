# MongoDB CRUD REST API

Esta API utiliza FastAPI para expor operações de CRUD sobre uma coleção de séries temporais no MongoDB.

## Requisitos

- Python 3.11+
- MongoDB 6+

## Configuração

1. Copie o arquivo `.env.example` para `.env` e ajuste os valores conforme o seu ambiente, definindo obrigatoriamente `API_ADMIN_TOKEN` (token administrador) e, opcionalmente, `ENABLE_TOKEN_CREATION_ROUTE` caso deseje habilitar a rota de criação de tokens.
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

## Autenticação e gerenciamento de tokens

Todas as rotas sob `/api` exigem o cabeçalho `X-API-Token`. Defina um valor para `API_ADMIN_TOKEN` no arquivo `.env` para obter um token com acesso completo. Opcionalmente é possível informar `X-Database-Name` em conjunto com o token de administrador para direcionar chamadas a outra base de dados; quando omitido, a aplicação utiliza o valor de `MONGODB_DATABASE`.

Quando `ENABLE_TOKEN_CREATION_ROUTE=true`, a rota `POST /api/tokens` fica disponível (ela não aparece na documentação pública) e permite emitir novos tokens persistidos na coleção definida por `API_TOKENS_COLLECTION`. Ao criar um token para uma base inexistente, o serviço cria automaticamente o banco e a coleção time-series configurada, garantindo que as próximas requisições já encontrem a estrutura necessária.

Exemplo de criação de token:

```bash
curl -X POST http://localhost:8000/api/tokens \
  -H "X-API-Token: ${API_ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
        "database": "validationsplugin",
        "description": "Token de pipeline"
      }'
```

A resposta conterá o campo `token` (exibido apenas uma vez); armazene-o com segurança. Todos os tokens são guardados no MongoDB utilizando hash SHA-256 e têm o campo `last_used_at` atualizado sempre que forem utilizados.

### Exemplo de escrita rápida via `curl`

```bash
curl -X POST http://localhost:8000/api/records \
  -H "X-API-Token: <seu-token>" \
  -H "Content-Type: application/json" \
  -d '{
        "acronym": "swe",
        "component": "automated-prr",
        "payload": {"healthcheck": true, "circuitbreak": true, "bulkhead": false, "ratelimit": false},
        "metadata": {"technology": "python"}
      }'
```

### Exemplo de busca

```bash
curl \
  -H "X-API-Token: <seu-token>" \
  "http://localhost:8000/api/records/search?field=source&value=swe&latest=true"
```

Qualquer campo armazenado pode ser utilizado na busca, inclusive campos aninhados usando dot-notation:

```bash
curl \
  -H "X-API-Token: <seu-token>" \
  "http://localhost:8000/api/records/search?field=payload.healthcheck&value=true&latest=true"
```

## Endpoints principais

Todos os endpoints abaixo exigem o cabeçalho `X-API-Token`.

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
