# 🛒 ShopBrasil Data Pipeline v3 (FakeStore API Integration)

Este repositório contém a engenharia de um pipeline de dados (ETL) robusto e totalmente dockerizado para o ecossistema **ShopBrasil**. A solução realiza a ingestão de dados brutos de produtos, processa métricas analíticas agregadas em paralelo e persiste os resultados de forma idempotente em um banco de dados relacional isolado.

## 🏗️ Arquitetura e Engenharia do Pipeline

O pipeline foi desenhado utilizando o **Apache Airflow 2.9.3 (TaskFlow API)**, explorando conceitos avançados de paralelismo e escalabilidade:

1. **Ingestão (`buscar_produtos`)**: Consome os dados de catálogo diretamente da API REST FakeStore.
2. **Orquestração Dinâmica (`extrair_categorias`)**: Identifica as categorias de produtos existentes em tempo de execução.
3. **Mapeamento Dinâmico (`calcular_metricas_por_categoria`)**: Utiliza a feature de `.expand()` do Airflow para criar tasks paralelas dinamicamente para cada categoria encontrada. Cada task calcula isoladamente: preço médio, preço mínimo, preço máximo e volumetria.
4. **Persistência Resiliente (`salvar_no_postgres`)**: Realiza a carga (Load) em uma tabela dedicada. Utiliza uma estratégia de **UPSERT (`ON CONFLICT DO UPDATE`)**, garantindo que o pipeline seja totalmente idempotente (pode rodar múltiplas vezes sem duplicar dados).

---

## 🛠️ Stack Tecnológica

* **Orquestrador:** Apache Airflow 2.9.3 (Python 3.11)
* **Banco de Metadados:** PostgreSQL 16 (`airflow-meta-db`)
* **Banco de Destino / Analytics:** PostgreSQL 16 (`airflow-lab-db`)
* **Infraestrutura:** Docker & Docker Compose

---

## 📦 Como Inicializar o Ambiente

### Pré-requisitos
* Docker e Docker Compose configurados no sistema.

### Inicialização da Infraestrutura

### 1. Clone este repositório para sua máquina local:
   ```bash
   git clone [https://github.com/jhsales/Lab-Airflow-atv1.git](https://github.com/jhsales/Lab-Airflow-atv1.git)
   cd Lab-Airflow-atv1
   ```
   ### 1.1 Suba a stack do Docker em modo background:
   ```bash
   docker compose up -d
   ````
   ### 1.2 Acesse o painel de controle do Airflow através do navegador:
   ```bash
   URL: http://localhost:8080
   ```
Credenciais: Usuário: admin | Senha: admin

### 2.⚙️ Configuração da Conexão com o Banco de Dados
Para que a Task final grave os dados corretamente, certifique-se de que a conexão postgres_default esteja apontando para o container de destino correto. Caso precise reinjetar a conexão via terminal, utilize o comando abaixo:
```bash
docker exec -it airflow-scheduler airflow connections add 'postgres_default' \
  --conn-type 'postgres' \
  --conn-host 'postgres-lab' \
  --conn-login 'lab' \
  --conn-password 'lab123' \
  --conn-schema 'labdb' \
  --conn-port 5432
```
### 3 📊 Estrutura do Banco de Dados Destino
Os insights processados são armazenados na tabela insight_precos_categoria com a seguinte estrutura lógica:
categoria (Chave Primária / Texto)

preco_medio (Numérico)

preco_minimo (Numérico)

preco_maximo (Numérico)

quantidade_produtos (Inteiro)

data_atualizacao (Timestamp)
