import logging
from datetime import datetime, timedelta
import pendulum
import requests

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuração do Timezone de Brasília
local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    'owner': 'Tech Lead - ShopBrasil',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='shopbrasil_v3',
    default_args=default_args,
    description='Pipeline de analise de produtos FakeStore com TaskFlow puro',
    schedule_interval=None,
    start_date=datetime(2026, 6, 1, tzinfo=local_tz),
    catchup=False,
    tags=['shopbrasil'],
)
def shopbrasil_dag():

    @task
    def buscar_produtos() -> list:
        url = "https://fakestoreapi.com/products"
        response = requests.get(url, timeout=20)
        return response.json()

    @task
    def extrair_categorias(produtos: list) -> list:
        return list(set([prod['category'] for prod in produtos if prod and 'category' in prod]))

    @task(pool='ecommerce_pool')
    def calcular_metricas_por_categoria(categoria: str, produtos: list) -> dict:
        # Garante o desempacotamento seguro caso a lista venha envelopada pelo expand
        if produtos and isinstance(produtos[0], list):
            produtos = produtos[0]
            
        prod_cat = [p for p in produtos if isinstance(p, dict) and p.get('category') == categoria]
        precos = [float(p['price']) for p in prod_cat if 'price' in p]
        quantidade = len(precos)
        
        preco_medio = sum(precos) / quantidade if quantidade > 0 else 0.0
        preco_min = min(precos) if quantidade > 0 else 0.0
        preco_max = max(precos) if quantidade > 0 else 0.0
        
        return {
            'categoria': categoria,
            'preco_medio': round(preco_medio, 2),
            'preco_min': round(preco_min, 2),
            'preco_max': round(preco_max, 2),
            'quantidade': quantidade
        }

    @task
    def salvar_no_postgres(**context):
        # Captura o contexto da task instance para puxar o XCom mapeado de forma explícita
        ti = context['task_instance']
        metricas_consolidadas = ti.xcom_pull(task_ids='calcular_metricas_por_categoria')
        
        execution_date = context['ds'] 
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS insight_precos_categoria (
            data_insight DATE,
            categoria VARCHAR(255),
            preco_medio NUMERIC(10,2),
            preco_min NUMERIC(10,2),
            preco_max NUMERIC(10,2),
            quantidade INT,
            PRIMARY KEY (data_insight, categoria)
        );
        """
        pg_hook.run(create_table_query)
        
        insert_query = """
        INSERT INTO insight_precos_categoria 
        (data_insight, categoria, preco_medio, preco_min, preco_max, quantidade)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (data_insight, categoria) 
        DO UPDATE SET 
            preco_medio = EXCLUDED.preco_medio,
            preco_min = EXCLUDED.preco_min,
            preco_max = EXCLUDED.preco_max,
            quantidade = EXCLUDED.quantidade;
        """
        
        rows = []
        if metricas_consolidadas:
            for m in metricas_consolidadas:
                if m and isinstance(m, dict):
                    rows.append((
                        execution_date, 
                        m.get('categoria'), 
                        m.get('preco_medio'), 
                        m.get('preco_min'), 
                        m.get('preco_max'), 
                        m.get('quantidade')
                    ))
        
        if rows:
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(insert_query, rows)
                    conn.commit()
            logging.info(f"📊 {len(rows)} registros salvos com sucesso de forma idempotente!")
        else:
            logging.warning("⚠️ Nenhum registro válido encontrado no XCom para inserção.")

    # -------------------------------------------------------------
    # Fluxo de Execução e Dependências
    # -------------------------------------------------------------
    dados_produtos = buscar_produtos()
    lista_categorias = extrair_categorias(dados_produtos)
    
    # Mapeamento dinâmico paralelo
    metricas = calcular_metricas_por_categoria.expand(
        categoria=lista_categorias,
        produtos=[dados_produtos]
    )
    
    # Define a dependência explícita via bitshift operator
    metricas >> salvar_no_postgres()

shopbrasil_dag_instance = shopbrasil_dag()