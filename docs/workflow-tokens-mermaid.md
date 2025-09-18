# Workflow Cross-Funcional: Rota /tokens

## Diagrama Mermaid

```mermaid
flowchart TD
    %% Definindo estilos
    classDef startEnd fill:#4CAF50,stroke:#2E7D32,stroke-width:2px,color:#fff
    classDef process fill:#2196F3,stroke:#1565C0,stroke-width:2px,color:#fff
    classDef decision fill:#FF9800,stroke:#E65100,stroke-width:2px,color:#fff
    classDef error fill:#F44336,stroke:#C62828,stroke-width:2px,color:#fff
    classDef database fill:#9C27B0,stroke:#6A1B9A,stroke-width:2px,color:#fff
    classDef token fill:#00BCD4,stroke:#00838F,stroke-width:2px,color:#fff
    
    %% Início do fluxo
    A[POST /tokens<br/>Body: database, description]:::startEnd
    
    %% Autenticação
    B[Validar adminToken]:::process
    C{Token Válido?}:::decision
    D[401 Unauthorized]:::error
    
    %% Verificação de Database
    E[Verificar se database existe]:::database
    F{Database existe?}:::decision
    G[Criar database]:::database
    
    %% Verificação de Collection
    H{TokenCollection existe?}:::decision
    I[Criar TokenCollection]:::database
    
    %% Geração de Token
    J[Gerar token aleatório]:::token
    K[Definir TTL/Expiração]:::token
    L[Escrever na TokenCollection]:::database
    
    %% Resposta
    M[Retornar 201 Created<br/>Payload com token]:::startEnd
    
    %% Conexões do fluxo
    A --> B
    B --> C
    C -->|Não| D
    C -->|Sim| E
    E --> F
    F -->|Não| G
    F -->|Sim| H
    G --> H
    H -->|Não| I
    H -->|Sim| J
    I --> J
    J --> K
    K --> L
    L --> M
```

## Diagrama Cross-Funcional com Swimlanes

```mermaid
flowchart TB
    subgraph "Lane 1: Client/API"
        A1[POST /tokens<br/>Request Body]
    end
    
    subgraph "Lane 2: Authentication"
        B1[Validar adminToken]
        B2{Token Válido?}
        B3[401 Unauthorized]
    end
    
    subgraph "Lane 3: Database Layer"
        C1[Verificar database]
        C2{Database existe?}
        C3[Criar database]
        C4{TokenCollection existe?}
        C5[Criar TokenCollection]
    end
    
    subgraph "Lane 4: Token Service"
        D1[Gerar token aleatório]
        D2[Definir TTL]
    end
    
    subgraph "Lane 5: Data Persistence"
        E1[Escrever TokenCollection]
    end
    
    subgraph "Lane 6: Response"
        F1[201 Created<br/>Response Payload]
    end
    
    %% Fluxo entre lanes
    A1 --> B1
    B1 --> B2
    B2 -->|Não| B3
    B2 -->|Sim| C1
    C1 --> C2
    C2 -->|Não| C3
    C2 -->|Sim| C4
    C3 --> C4
    C4 -->|Não| C5
    C4 -->|Sim| D1
    C5 --> D1
    D1 --> D2
    D2 --> E1
    E1 --> F1
    
    %% Estilos
    classDef startEnd fill:#4CAF50,stroke:#2E7D32,stroke-width:2px,color:#fff
    classDef process fill:#2196F3,stroke:#1565C0,stroke-width:2px,color:#fff
    classDef decision fill:#FF9800,stroke:#E65100,stroke-width:2px,color:#fff
    classDef error fill:#F44336,stroke:#C62828,stroke-width:2px,color:#fff
    classDef database fill:#9C27B0,stroke:#6A1B9A,stroke-width:2px,color:#fff
    classDef token fill:#00BCD4,stroke:#00838F,stroke-width:2px,color:#fff
    
    class A1,F1 startEnd
    class B1,C1,C3,C5,E1 process
    class B2,C2,C4 decision
    class B3 error
    class D1,D2 token
```

## Payload de Exemplo

### Request
```json
{
  "database": "validationsplugin",
  "description": "commandline_tool"
}
```

### Response (201 Created)
```json
{
  "token": "2922f524f758fcc8f0c08bff93771a1e",
  "database": "validationsplugin", 
  "description": "commandline_tool",
  "created_at": "2025-09-18T13:31:49.779353Z",
  "last_used_at": null,
  "expires_at": null
}
```

## Requisitos Técnicos

- **Autenticação**: adminToken obrigatório no header Authorization
- **Escopo**: Tokens limitados à database especificada
- **TTL**: Configurável para expiração automática ou permanente
- **Persistência**: Armazenamento na collection TokenCollection da database correspondente
- **Response**: HTTP 201 com metadados completos do token criado