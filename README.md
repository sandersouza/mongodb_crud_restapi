# Data Abstration Layer
Esta API utiliza FastAPI para expor operações de CRUD usando payloads flexíveis, persistindo o dado em backends variados como MongoDB, PostGreSQL, Prometheus e outros de forma extensível.

## Requisitos
- Python 3.11+
- MongoDB 6+

## Configuração
1. Copie o arquivo `.env.example` para `.env` e ajuste os valores conforme o seu ambiente, definindo obrigatoriamente `API_ADMIN_TOKEN` (token administrador) e, opcionalmente, `SHOW_TOKEN_CREATION_ROUTE` caso deseje exibir na documentação as rotas administrativas de tokens.
2. Crie um ambiente virtual e instale as dependências:
```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Execução
```bash
. .env
./start.sh
```

## Autenticação e contexto de banco de dados
Todas as rotas sob `/api` exigem o cabeçalho `Authorization` no formato `Bearer <token>`. Algumas observações importantes:

- `API_ADMIN_TOKEN` (definido no `.env`) concede acesso administrativo completo.
- Ao usar o token de administrador em rotas que manipulam registros (`/api/records`), informe também `X-Database-Name: <nome-da-base>` para indicar qual base MongoDB deve ser utilizada.
- Tokens criados via `/api/tokens` ficam vinculados a uma base específica e não exigem o cabeçalho `X-Database-Name`, pois a resolução da base é automática.

### Tokens administrativos e preparação de infraestrutura
Com o token do administrador, o serviço consegue criar automaticamente a base solicitada, a coleção time-series configurada e a coleção definida em `API_TOKENS_COLLECTION`. Dessa forma, é possível fornecer credenciais para times ou sistemas mesmo quando a estrutura ainda não existe.

### Tokens de aplicação
Cada token de aplicação é armazenado com hash SHA-256, registra o campo `last_used_at` a cada requisição e pode receber um tempo de expiração (`expires_in_seconds`). Quando informado, esse tempo gera um `expires_at` e o serviço cria um índice TTL para eliminação automática dos tokens expirados. Guarde o valor retornado no ato da criação — ele não é exibido novamente.

## Guia de consumo via `curl`
Os exemplos a seguir assumem a API disponível em `http://localhost:8000`. Ajuste URLs e cabeçalhos conforme o seu ambiente.

### Verificar disponibilidade (`GET /healthz`)
Confirma se o serviço está saudável e pronto para receber requisições.

```bash
curl http://localhost:8000/healthz
```

### Gerenciar tokens administrativos
As rotas abaixo exigem `Authorization: Bearer ${API_ADMIN_TOKEN}`. Defina `SHOW_TOKEN_CREATION_ROUTE=true` no `.env` se quiser que elas apareçam em `/docs`.

#### Criar token (`POST /api/tokens`)
Emite um token vinculado à base `validationsplugin`. Quando a base ainda não existe, a API cria toda a estrutura necessária antes de persistir o token.

```bash
export API_ADMIN_TOKEN="<token-administrador>"

curl -X POST http://localhost:8000/api/tokens \
  -H "Authorization: Bearer ${API_ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
        "database": "validationsplugin",
        "description": "Token de pipeline",
        "expires_in_seconds": 3600
      }'
```

Resposta esperada (`201 Created`):

```json
{
  "token": "c6k9...",
  "database": "validationsplugin",
  "description": "Token de pipeline",
  "created_at": "2024-01-01T00:00:00Z",
  "last_used_at": null,
  "expires_at": "2024-01-01T01:00:00Z"
}
```

#### Listar tokens (`GET /api/tokens`)

Retorna todos os tokens emitidos. Utilize o parâmetro opcional `database` para filtrar por uma base específica.

```bash
curl "http://localhost:8000/api/tokens?database=validationsplugin" \
  -H "Authorization: Bearer ${API_ADMIN_TOKEN}"
```

#### Revogar token (`DELETE /api/tokens/{database}/{token_id}`)

Remove um token específico com base no `_id` obtido na listagem.

```bash
curl -X DELETE http://localhost:8000/api/tokens/validationsplugin/64b000000000000000000000 \
  -H "Authorization: Bearer ${API_ADMIN_TOKEN}"
```

### Trabalhar com registros de séries temporais
Após emitir um token de aplicação, exporte-o para facilitar o uso nos exemplos:

```bash
export ACCESS_TOKEN="<token-de-acesso>"
```

Caso prefira usar o token administrador para essas rotas, acrescente `-H "X-Database-Name: <nome-da-base>"` em cada comando.

Para habilitar a remoção automática de documentos antigos, informe `expires_in_seconds` ao criar o registro. O serviço grava um `expires_at` correspondente (com base no `timestamp` informado) e mantém um índice TTL para que o MongoDB exclua automaticamente os documentos expirados. Quando o campo é omitido ou `0`, o registro permanece indefinidamente.

#### Criar registro (`POST /api/records`)
Persiste um novo registro de série temporal. O campo `acronym` é um alias para `source`; utilize o que for mais conveniente. O serviço garante que a coleção time-series exista e cria índices necessários.

```bash
curl -X POST http://localhost:8000/api/records \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
        "acronym": "swe",
        "component": "automated-prr",
        "payload": {
          "healthcheck": true,
          "circuitbreak": true,
          "bulkhead": false,
          "ratelimit": false
        },
        "metadata": {"technology": "python"},
        "expires_in_seconds": 3600
      }'
```

A resposta (`201 Created`) retorna o documento completo, incluindo `id` (ObjectId em formato de string), `timestamp` e, quando configurado, `expires_at` em ISO-8601.

#### Listar registros (`GET /api/records`)
Retorna os registros mais recentes primeiro. Use `limit` (1-1000) e `skip` para paginação simples.

```bash
curl "http://localhost:8000/api/records?limit=25&skip=0" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}"
```

#### Buscar por filtros (`GET /api/records/search`)
Permite buscar por qualquer campo (inclusive aninhado com dot-notation) e restringir por janela temporal. Quando `latest=true`, apenas o registro mais recente é retornado.

```bash
curl "http://localhost:8000/api/records/search?field=payload.healthcheck&value=true&latest=true" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}"
```

A resposta traz `latest` (booleano indicando se somente o item mais recente foi retornado), `count` e a lista `items`.

#### Consultar por ID (`GET /api/records/{record_id}`)
Recupera um registro específico a partir do `id` retornado nas operações anteriores.

```bash
curl http://localhost:8000/api/records/64b000000000000000000000 \
  -H "Authorization: Bearer ${ACCESS_TOKEN}"
```

#### Atualizar registro (`PUT /api/records/{record_id}`)
Atualiza os campos informados. O corpo aceita qualquer subconjunto de `source/acronym`, `component`, `payload`, `metadata` e `timestamp`.

```bash
curl -X PUT http://localhost:8000/api/records/64b000000000000000000000 \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
        "component": "automated-prr",
        "metadata": {"technology": "python", "owner": "sre"}
      }'
```

#### Excluir registro (`DELETE /api/records/{record_id}`)
Remove um documento da coleção. Em caso de sucesso, a API responde com `204 No Content`.

```bash
curl -X DELETE http://localhost:8000/api/records/64b000000000000000000000 \
  -H "Authorization: Bearer ${ACCESS_TOKEN}"
```

## Endpoints principais
| Método | Rota | Descrição |
| --- | --- | --- |
| GET | `/healthz` | Verifica se o serviço está disponível. |
| POST | `/api/tokens` | Emite tokens vinculados a uma base (exibido em `/docs` quando `SHOW_TOKEN_CREATION_ROUTE=true`). |
| GET | `/api/tokens` | Lista tokens emitidos, com filtro opcional por base. |
| DELETE | `/api/tokens/{database}/{token_id}` | Revoga um token específico. |
| POST | `/api/records` | Cria um registro time-series com criação automática da infraestrutura. |
| GET | `/api/records` | Lista registros ordenados do mais recente para o mais antigo. |
| GET | `/api/records/search` | Pesquisa por campo, valores e janelas temporais. |
| GET | `/api/records/{record_id}` | Recupera um registro específico pelo identificador. |
| PUT | `/api/records/{record_id}` | Atualiza parcialmente um registro existente. |
| DELETE | `/api/records/{record_id}` | Remove um registro da coleção. |

Consulte a documentação automática em `http://localhost:8000/docs` para descrições completas dos modelos e exemplos adicionais.

## Docker
Um Dockerfile está disponível para facilitar a conteinerização. Para construir a imagem de produção:

```bash
docker build -t datalayerAbstraction .
```

Em seguida execute:
```bash
docker run --env-file .env -p 8000:8000 datalayerAbstraction
```

Certifique-se de disponibilizar o MongoDB acessível para o container (por exemplo, via rede Docker).

## Documentação da API / Workflows
### Workflow Cross-funcional
- [Criação de API Token](./docs/workflow-tokens-mermaid.md)
