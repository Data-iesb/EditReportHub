import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.io as pio

ocorrencias = pd.read_csv('s3://dataiesb/datasets/reports/1/ocorrencias_ride_2020_2024_novembro.csv')

# Remover colunas desnecessárias
ocorrencias.drop(['agente', 'arma', 'faixa_etaria', 'total_peso', 'formulario', 'abrangencia'], axis=1, inplace=True)


ocorrencias['codigo_municipio_dv'] = ocorrencias['codigo_municipio_dv'].astype(str)
ocorrencias['ano'] = ocorrencias['ano'].astype(int)

# Título na barra lateral
st.sidebar.title("Menu")

# Adicionar a seleção de Menu na barra lateral
pagina = st.sidebar.selectbox(
    "Escolha a Página",
    ["Gráficos", "Tabela de Ocorrências"]
)

# Título na barra lateral abaixo do Menu
st.sidebar.title("Filtros")

# Layout para as seleções na parte superior da página principal
col1, col2 = st.columns(2)

# Adicionar a opção "Todos os Eventos" à lista de eventos
eventos_disponiveis = sorted(ocorrencias["evento"].unique())

# Seleção de Evento com múltipla escolha
eventos_selecionados = col1.multiselect(
    "Selecione o(s) Evento(s)",
    ["Todos"] + eventos_disponiveis,
    default=["Todos"]
)

# Seleção de Gênero
genero = col2.selectbox(
    "Selecione o Gênero",
    ["Todos", "Feminino", "Masculino"]
)

# Adicionar a opção "Todos os Municípios" à lista de municípios
municipios_disponiveis = sorted(ocorrencias["municipio"].unique())

# Seleção de Município com múltipla escolha
municipios_selecionados = st.sidebar.multiselect(
    "Selecione o(s) Município(s) da RIDE",
    ["Todos"] + municipios_disponiveis,
    default=["Todos"]
)

# Adicionar a opção "Todos os Anos" à lista de anos
anos_disponiveis = sorted(ocorrencias["ano"].unique())

# Seleção de Ano com múltipla escolha
anos_selecionados = st.sidebar.multiselect(
    "Selecione o(s) Ano(s)",
    ["Todos"] + anos_disponiveis,
    default=["Todos"]
)

# Criar seleção de meses com múltipla escolha baseada nos anos selecionados
if "Todos" in anos_selecionados:
    meses_disponiveis = sorted(ocorrencias["mes"].unique())
else:
    meses_disponiveis = sorted(ocorrencias[ocorrencias["ano"].isin(anos_selecionados)]["mes"].unique())

meses_selecionados = st.sidebar.multiselect(
    "Selecione o(s) Mês(es)",
    ["Todos"] + meses_disponiveis,
    default=["Todos"]
)

# Filtrar os dados com base nas seleções feitas
ocorrencia_filtrada = ocorrencias

# Filtrar por eventos
if "Todos" not in eventos_selecionados:
    ocorrencia_filtrada = ocorrencia_filtrada[ocorrencia_filtrada["evento"].isin(eventos_selecionados)]

# Filtrar por gênero
if genero == "Feminino":
    ocorrencia_filtrada = ocorrencia_filtrada[ocorrencia_filtrada["feminino"] > 0]
elif genero == "Masculino":
    ocorrencia_filtrada = ocorrencia_filtrada[ocorrencia_filtrada["masculino"] > 0]

# Filtrar por municípios
if "Todos" not in municipios_selecionados:
    ocorrencia_filtrada = ocorrencia_filtrada[ocorrencia_filtrada["municipio"].isin(municipios_selecionados)]

# Filtrar por anos
if "Todos" not in anos_selecionados:
    ocorrencia_filtrada = ocorrencia_filtrada[ocorrencia_filtrada["ano"].isin(anos_selecionados)]

# Filtrar por meses
if "Todos" not in meses_selecionados:
    ocorrencia_filtrada = ocorrencia_filtrada[ocorrencia_filtrada["mes"].isin(meses_selecionados)]

# Exibir conteúdo de acordo com a página selecionada
if pagina == "Gráficos":
    st.title("Análise Gráfica das Ocorrências Criminais")

    # Distribuição de Gênero por Ocorrência (Gráfico de Donut)
    col1, col2 = st.columns(2)

    # Agrupando por gênero para a distribuição
    genero_distribuicao = ocorrencia_filtrada[['feminino', 'masculino']].sum().reset_index()
    genero_distribuicao.columns = ['Genero', 'Total']

    fig_donut = px.pie(genero_distribuicao, names='Genero', values='Total', hole=0.4,
                       title="Distribuição de Gênero por Ocorrência",
                       color_discrete_sequence=px.colors.qualitative.G10)

    fig_donut.update_traces(textinfo='percent+label')
    col1.plotly_chart(fig_donut, use_container_width=True)

    # Total de Vítimas por Gênero por Ano (Gráfico de Colunas Duplas)
    vitimas_por_ano = ocorrencia_filtrada.groupby(['ano']).agg({'feminino': 'sum', 'masculino': 'sum'}).reset_index()

    fig_colunas = px.bar(vitimas_por_ano, x='ano', y=['feminino', 'masculino'],
                         title="Total de Vítimas por Gênero por Ano",
                         labels={'value': 'Total de Vítimas', 'variable': 'Gênero'},
                         barmode='group',
                         color_discrete_sequence=px.colors.qualitative.Set1)

    col2.plotly_chart(fig_colunas, use_container_width=True)

    # Gráfico de Linha de Quantidade de Vítimas por Tempo
    st.subheader("Quantidade de Vítimas ao Longo do Tempo")

    # Preparar os dados para o gráfico de linha
    # Agrupar por ano e mês e somar a coluna total_vitimas
    vitimas_tempo = ocorrencia_filtrada.groupby(['ano', 'mes'])['total_vitimas'].sum().reset_index(name='total_vitimas')

    # Criar uma coluna de data com o primeiro dia do mês
    vitimas_tempo['data'] = pd.to_datetime(
        vitimas_tempo['ano'].astype(str) + '-' + vitimas_tempo['mes'].astype(str).str.zfill(2) + '-01',
        format='%Y-%m-%d'
    )

    # Criar o gráfico de linha
    fig_linha = px.line(vitimas_tempo, x='data', y='total_vitimas',
                        title="Quantidade de Vítimas ao Longo do Tempo",
                        labels={'data': 'Data', 'total_vitimas': 'Quantidade de Vítimas'},
                        markers=True)

    st.plotly_chart(fig_linha, use_container_width=True)

elif pagina == "Tabela de Ocorrências":
    st.title("Tabela de Ocorrências Criminais Filtradas")

    # Exibir a tabela com as ocorrências filtradas
    st.dataframe(ocorrencia_filtrada)


