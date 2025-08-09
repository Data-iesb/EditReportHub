import boto3
import json
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import plotly.express as px

# ========================
# Obter segredo da AWS
# ========================
def get_secret(secret_name):
    client = boto3.client('secretsmanager', region_name='us-east-1')
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return json.loads(get_secret_value_response['SecretString'])

# Segredo
secret = get_secret("rds-read-only")

DB_HOST = secret.get("host")
DB_NAME = secret.get("dbname")
DB_USER = secret.get("username")
DB_PASSWORD = secret.get("password")
DB_PORT = secret.get("port")

# ========================
# Criar URL do SQLAlchemy
# ========================
db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

engine = create_engine(db_url)

# ========================
# Streamlit + Dashboard
# ========================
st.set_page_config(page_title="Dashboard PIB", layout="wide")
st.title("📈 Dashboard Interativo do PIB Brasileiro")

df = pd.read_sql_table(
    table_name="pib_municipios_brasil",
    con=engine,
    schema="public"
)

df['ano_pib'] = df['ano_pib'].astype(int)

# PIB Total por ano
pib_p_ano = df.groupby('ano_pib')['vl_pib'].sum().reset_index()
pib_p_ano['vl_pib_bilhoes'] = pib_p_ano['vl_pib'] / 1_000_000_000

fig_total = px.line(
    pib_p_ano,
    x='ano_pib',
    y='vl_pib_bilhoes',
    title='PIB Total do Brasil por Ano (em bilhões R$)',
    labels={'ano_pib': 'Ano', 'vl_pib_bilhoes': 'PIB (Bilhões R$)'},
    color_discrete_sequence=['dodgerblue'],
    markers=True,
    template="plotly_white"
)

pib_p_ano['PIB (Bilhões R$)'] = pib_p_ano['vl_pib_bilhoes'].apply(lambda x: f'R$ {x:,.2f}B')
tabela = pib_p_ano[['ano_pib', 'PIB (Bilhões R$)']].rename(columns={'ano_pib': 'Ano'})

# Abas
aba1, aba2 = st.tabs(["📊 PIB Total do Brasil", "🌎 Análise por Região"])

with aba1:
    st.plotly_chart(fig_total, use_container_width=True)
    st.subheader("Tabela do PIB do Brasil por Ano")
    st.dataframe(tabela, use_container_width=True)

with aba2:
    st.subheader("Filtros Interativos para Análise por Região")

    ano_min = df['ano_pib'].min()
    ano_max = df['ano_pib'].max()
    regioes = ['Geral'] + sorted(df['nome_regiao'].unique())

    col_f1, col_f2 = st.columns([1, 2])

    with col_f1:
        regiao_selecionada = st.selectbox("Selecione a Região:", regioes, index=1)

    with col_f2:
        faixa_anos = st.slider(
            "Selecione a Faixa de Ano:",
            min_value=ano_min,
            max_value=ano_max,
            value=(ano_min, ano_max),
            step=1
        )

    ano_inicio, ano_fim = faixa_anos

    if regiao_selecionada == 'Geral':
        dados_linha = df.groupby('ano_pib')['vl_pib'].sum().reset_index()
        titulo_linha = "Valor do PIB por Ano - Todas as Regiões"

        dados_pizza = df[(df['ano_pib'] >= ano_inicio) & (df['ano_pib'] <= ano_fim)]
        dados_pizza = dados_pizza.groupby('nome_regiao')['vl_pib'].sum().reset_index()

        fig_pizza = px.pie(
            dados_pizza,
            names='nome_regiao',
            values='vl_pib',
            title=f"Distribuição do PIB por Região ({ano_inicio}-{ano_fim})",
            template='plotly_white'
        )
    else:
        dados_linha = df[
            (df['nome_regiao'] == regiao_selecionada) &
            (df['ano_pib'] >= ano_inicio) &
            (df['ano_pib'] <= ano_fim)
        ].groupby('ano_pib')['vl_pib'].sum().reset_index()

        titulo_linha = f"Valor do PIB por Ano - Região {regiao_selecionada}"

        dados_pizza = df[
            (df['nome_regiao'] == regiao_selecionada) &
            (df['ano_pib'] >= ano_inicio) &
            (df['ano_pib'] <= ano_fim)
        ].groupby('nome_uf')['vl_pib'].sum().reset_index()

        fig_pizza = px.pie(
            dados_pizza,
            names='nome_uf',
            values='vl_pib',
            title=f"Distribuição do PIB por Estado - {regiao_selecionada} ({ano_inicio}-{ano_fim})",
            template='plotly_white'
        )

    fig_linha = px.line(
        dados_linha,
        x='ano_pib',
        y='vl_pib',
        title=titulo_linha,
        labels={'ano_pib': 'Ano', 'vl_pib': 'PIB Total'},
        template='plotly_white',
        markers=True
    )

    col1, col2 = st.columns(2)
    col1.plotly_chart(fig_linha, use_container_width=True)
    col2.plotly_chart(fig_pizza, use_container_width=True)

