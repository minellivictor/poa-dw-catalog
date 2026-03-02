# POA DW Catalog

Aplicação web para catálogo de metadados do DW da Prefeitura de Porto Alegre.

## Objetivo

Disponibilizar uma interface simples para busca de tabelas e colunas nos schemas:

- bronze
- silver
- gold

O filtro de camada é opcional: por padrão, a busca considera todas as camadas (bronze/silver/gold).

A aplicação trabalha apenas com metadados (sem leitura de dados sensíveis).

## Stack

- Python
- FastAPI
- Jinja2
- SQLAlchemy
- SQLite

## Executando localmente

1. Criar e ativar ambiente virtual:

```bash
python -m venv .venv
source .venv/bin/activate
```

2. Instalar dependências:

```bash
pip install -r requirements.txt
```

3. Iniciar o servidor:

```bash
uvicorn src.main:app --reload
```

4. Abrir no navegador:

- [http://localhost:8000](http://localhost:8000)

Critério esperado: ao acessar a URL, a página deve mostrar o título **POA DW Catalog** e um campo de busca estático.

## Qualidade

Executar antes de subir mudanças:

```bash
ruff check .
pytest
```
