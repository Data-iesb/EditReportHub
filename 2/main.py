import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
import boto3

import boto3
import json
from botocore.exceptions import ClientError



# ========== ESTILO CUSTOMIZADO ==========
# ========== ESTILO CUSTOMIZADO ==========

st.markdown("""
    <style>

    .red-banner {
        background-color: #D13F42;
        padding: 24px 0 12px 0;
        margin-bottom: 0px;
        text-align: center;
        color: white;
        font-size: 2.2rem;
        font-weight: bold;
        letter-spacing: 1px;
        border-radius: 0 0 12px 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }

    .stTabs [data-baseweb="tab-list"] {
        justify-content: center;
    }

    .footer {
        text-align: center;
        color: #666;
        margin-top: 32px;
        font-size: 1rem;
        padding-bottom: 12px;
    }

    </style>
""", unsafe_allow_html=True)


# ========== TOPO DO SITE ==========
st.markdown("<div class='red-banner'>Dashboard de Análise do PIB dos Municípios do Brasil</div>", unsafe_allow_html=True)
st.markdown("")

# ========== CONEXÃO E CARREGAMENTO DOS DADOS ==========

def get_secret(secret_name):
    client = boto3.client('secretsmanager')
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    dct = json.loads(get_secret_value_response['SecretString'])
    return dct

secret = get_secret("rds-read-only")

DB_HOST = secret.get("host")
DB_NAME = secret.get("dbname")
DB_USER = secret.get("username")
DB_PASSWORD = secret.get("password")
DB_PORT = secret.get("port")

@st.cache_data
def get_data():
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        # Filtro SQL para dados de 2010 em diante
        query = """
            SELECT ano_pib, vl_pib, nome_regiao, nome_uf, nome_municipio
            FROM pib_municipios_brasil
            WHERE CAST(ano_pib AS INTEGER) >= 2010
        """
        # Usa cursor server-side para fetch_size funcionar
        cursor = conn.cursor(name='pib_cursor')
        cursor.itersize = 10000  # Tamanho do fetch em cada rodada (ajuste conforme necessário)
        cursor.execute(query)
        # Lê o resultado em chunks, mas junta tudo no final (pandas concatena)
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        cursor.close()
        return df
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados ou buscar dados: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()
def format_currency(value):
    return f"R$ {value/1_000_000_000:.2f}B"

def create_line_chart(data, x_col, y_col, title, x_label, y_label, color_col=None):
    if color_col:
        fig = px.line(data, x=x_col, y=y_col, color=color_col,
                     title=title,
                     labels={x_col: x_label, y_col: y_label})
    else:
        fig = px.line(data, x=x_col, y=y_col,
                     title=title,
                     labels={x_col: x_label, y_col: y_label})
    fig.update_layout(
        title_font_size=16,
        xaxis_title_font_size=14,
        yaxis_title_font_size=14,
        legend_title_font_size=12
    )
    return fig

def create_pie_chart(data, values_col, names_col, title):
    fig = px.pie(data, values=values_col, names=names_col, title=title)
    fig.update_layout(
        title_font_size=16,
        showlegend=True
    )
    return fig

def footer():
    st.markdown("""
    <div class='footer'>
        Dashboard desenvolvido para o Projeto Big Data - IESB<br>
        Dados: PIB dos Municípios do Brasil (2010-2021)
    </div>
    """, unsafe_allow_html=True)

# ========== CARREGAMENTO DOS DADOS ==========
df = get_data()

if not df.empty:
    # ========== ABAS ==========
    tab1, tab2, tab3 = st.tabs([
        "📊 Evolução do PIB (2010-2021)",
        "🌎 Evolução do PIB por Região",
        "ℹ️ Informações do Dataset"
    ])

    # ========== ABA 1 ==========
    with tab1:
        st.markdown("Esta seção apresenta a evolução do PIB total do Brasil ao longo dos anos.")
        pib_por_ano = df.groupby('ano_pib')['vl_pib'].sum().reset_index()
        pib_por_ano['vl_pib_bilhoes'] = pib_por_ano['vl_pib'] / 1_000_000_000

        col1, col2 = st.columns([1,2])
        with col1:
            tabela_pib_ano = pib_por_ano.copy()
            tabela_pib_ano['Valor do PIB (Bilhões)'] = tabela_pib_ano['vl_pib_bilhoes'].apply(lambda x: f"R$ {x:.2f}B")
            tabela_pib_ano = tabela_pib_ano[['ano_pib', 'Valor do PIB (Bilhões)']].rename(columns={'ano_pib': 'Ano'})
            st.subheader("📋 Dados Detalhados do PIB por Ano")
            st.dataframe(tabela_pib_ano, use_container_width=True, hide_index=True)
        with col2:
            fig1 = create_line_chart(
                data=pib_por_ano,
                x_col='ano_pib',
                y_col='vl_pib_bilhoes',
                title='Valor do PIB (*1.000) por Ano do PIB',
                x_label='Ano do PIB',
                y_label='Valor do PIB (*1.000) (Bilhões)'
            )
            st.plotly_chart(fig1, use_container_width=True)
        footer()

    # ========== ABA 2 ==========
    with tab2:
        st.markdown("Esta seção permite análise interativa do PIB por região e unidade federativa.")
        regioes = sorted(df['nome_regiao'].unique())
        selected_regiao = st.selectbox("🔍 Selecione a Região:", regioes, key="regiao_selector")
        df_filtered_regiao = df[df['nome_regiao'] == selected_regiao]
        pib_por_ano_uf = df_filtered_regiao.groupby(['ano_pib', 'nome_uf'])['vl_pib'].sum().reset_index()
        pib_por_ano_uf['vl_pib_bilhoes'] = pib_por_ano_uf['vl_pib'] / 1_000_000_000

        col1, col2 = st.columns([2,1])
        with col1:
            fig2 = create_line_chart(
                data=pib_por_ano_uf,
                x_col='ano_pib',
                y_col='vl_pib_bilhoes',
                title=f'Valor do PIB (*1.000) por Ano e UF - Região {selected_regiao}',
                x_label='Ano do PIB',
                y_label='Valor do PIB (*1.000) (Bilhões)',
                color_col='nome_uf'
            )
            st.plotly_chart(fig2, use_container_width=True)
        with col2:
            anos_disponiveis = sorted(df_filtered_regiao['ano_pib'].unique())
            selected_ano = st.select_slider(
                "📅 Selecione o Ano para o Gráfico de Pizza:",
                options=anos_disponiveis,
                value=anos_disponiveis[-1],
                key="ano_selector"
            )
            pib_por_uf_ano = df_filtered_regiao[df_filtered_regiao['ano_pib'] == selected_ano].groupby('nome_uf')['vl_pib'].sum().reset_index()
            pib_por_uf_ano['vl_pib_bilhoes'] = pib_por_uf_ano['vl_pib'] / 1_000_000_000
            fig3 = create_pie_chart(
                data=pib_por_uf_ano,
                values_col='vl_pib_bilhoes',
                names_col='nome_uf',
                title=f'Distribuição do PIB por UF - Região {selected_regiao} ({selected_ano})'
            )
            st.plotly_chart(fig3, use_container_width=True)
        footer()

    # ========== ABA 3 ==========
    with tab3:
        st.subheader("ℹ️ Informações sobre o Dataset")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total de Registros", f"{len(df):,}")
        with col2:
            st.metric("Período dos Dados", f"{df['ano_pib'].min()} - {df['ano_pib'].max()}")
        with col3:
            st.metric("Número de Municípios", f"{df['nome_municipio'].nunique():,}")
        with col4:
            st.metric("Número de UFs", f"{df['nome_uf'].nunique()}")

        st.subheader("📈 Resumo Estatístico do PIB")
        # PIB médio por UF: soma do PIB por UF em cada ano, depois média das UFs
        resumo_stats = df.groupby(['ano_pib', 'nome_uf'])['vl_pib'].sum().reset_index()
        resumo_stats_ano = resumo_stats.groupby('ano_pib').agg(
            pib_total=('vl_pib', 'sum'),
            pib_medio_uf=('vl_pib', 'mean'),
            desvio_padrao=('vl_pib', 'std')
        ).reset_index()
        resumo_stats_ano.columns = ['Ano', 'PIB Total', 'PIB Médio por UF', 'Desvio Padrão']
        for col in ['PIB Total', 'PIB Médio por UF', 'Desvio Padrão']:
            resumo_stats_ano[col] = resumo_stats_ano[col].apply(format_currency)
        st.dataframe(resumo_stats_ano, use_container_width=True, hide_index=True)
        footer()

else:
    st.error("❌ Não foi possível carregar os dados. Verifique a conexão com o banco de dados.")
    st.info("Verifique se:")
    st.markdown("""
    - A conexão com a internet está funcionando
    - As credenciais do banco de dados estão corretas
    - O servidor do banco de dados está acessível
    """)
