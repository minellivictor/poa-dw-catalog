# POA DW Catalog

Catálogo de Dados do Data Warehouse (DW) da Prefeitura de Porto Alegre.

Aplicação web para navegação e busca por tabelas e colunas nos schemas:

- bronze
- silver
- gold

O objetivo é oferecer um "Atlan-lite" interno, permitindo que servidores e equipe técnica encontrem rapidamente:

- Onde está um dado
- Em qual camada ele se encontra
- Qual o tipo e estrutura da tabela
- Quem é o responsável (owner)
- Tags e anotações funcionais

---


## Objetivo do Projeto

Construir um catálogo de metadados do DW que:

- Faça introspecção automática do PostgreSQL
- Armazene apenas metadados (não dados sensíveis)
- Permita busca por palavras-chave
- Permita organização por tags e owners
- Reduza dependência da STI para entendimento de bases

---

## Segurança e LGPD

Esta aplicação:

- NÃO copia dados do DW
- NÃO lê conteúdo das tabelas
- NÃO armazena registros de contribuintes
- Apenas consulta metadados via `information_schema` e `pg_catalog`

---

## Stack Tecnológica

- Python 3.11+
- FastAPI
- Jinja2
- SQLAlchemy
- SQLite (catálogo local)
- PostgreSQL (fonte de metadados)
- pytest
- ruff

---

# Estrutura Esperada

src/
main.py        # FastAPI app
sync.py        # Sincronização de metadados do DW
models.py      # Modelos SQLAlchemy
database.py    # Configuração do SQLite
templates/
index.html
search.html
table.html
tests/
requirements.txt
.env.example
AGENTS.md

---

## Setup Local

### 1. Criar ambiente virtual

```bash
python -m venv .venv
source .venv/bin/activate
