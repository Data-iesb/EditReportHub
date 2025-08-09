import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path
import plotly.graph_objects as go
import geopandas as gpd
import requests

@st.cache_data
def get_data(path):
    DATA_FILE = f"s3://dataiesb/datasets/reports/4/{path}"
    return pd.read_csv(DATA_FILE)

file_paths = {
    'df_faixa_etaria': 'tabela_doc_faixa_etaria.csv',
    'df_cor_raca': 'tabela_doc_cor_raca.csv',
    'df_sexo': 'tabela_doc_sexo.csv',
    'df_escolaridade': 'tabela_doc_escol.csv',
    'df_tprede': 'tabela_tp_rede.csv',
    'df_acesso_internet': 'tabela_acesso_internet.csv',
    'df_repositorio_inst': 'tabela_repositorio_inst.csv',
    'df_repo': 'tabela_uf.csv',
    'df_uf': 'tabela_uf.csv',
    'df_turnos': 'qtd_total_vaga.csv',
    'df_concluintes': 'qtd_total_concluintes.csv',
    'df_escol_cor': 'tabela_doc_completa.csv',
    'df_tabela_mapa': 'tabela_mapa.csv'
}

dataframes = {name: get_data(path) for name, path in file_paths.items()}

df_faixa_etaria = dataframes['df_faixa_etaria']
df_cor_raca = dataframes['df_cor_raca']
df_sexo = dataframes['df_sexo']
df_escolaridade = dataframes['df_escolaridade']
df_tprede = dataframes['df_tprede']
df_acesso_internet = dataframes['df_acesso_internet']
df_repositorio_inst = dataframes['df_repositorio_inst']
df_repo = dataframes['df_repo']
df_uf = dataframes['df_uf']
df_turnos = dataframes['df_turnos']
df_concluintes = dataframes['df_concluintes']
df_escol_cor = dataframes['df_escol_cor']
df_tabela_mapa = dataframes['df_tabela_mapa']

def pegar_frequencias(
    df: pd.DataFrame,
    coluna: str,
    nome_coluna_1: str,
    nome_coluna_2: str
) -> tuple[pd.Series, pd.DataFrame]:
    """
    Calcula a frequência dos valores de uma coluna em um DataFrame, 
    cria um DataFrame com índice resetado e renomeia as colunas.

    Parâmetros:
    - df: DataFrame onde a coluna está presente.
    - coluna: Nome da coluna para calcular a frequência.
    - nome_coluna_1: Nome para a primeira coluna do DataFrame de resultado (ex: categorias).
    - nome_coluna_2: Nome para a segunda coluna do DataFrame de resultado (ex: frequências).

    Retorna:
    - frequencia_total: Série com a frequência dos valores (ordenada pelo índice).
    - frequencia_index: DataFrame com índice resetado e colunas renomeadas.
    """

    frequencia_total = df[coluna].value_counts().sort_index()
    frequencia_index = frequencia_total.reset_index()
    frequencia_index.columns = [nome_coluna_1, nome_coluna_2]

    return frequencia_total, frequencia_index

'''
# Análise dos dados do ensino superior do Brasil :bar_chart:

A educação é a base para a formação de bons profissionais. 
Com isso em mente, este dashboard tem como objetivo analisar os dados das universidades e 
cursos que integram a Região Integrada de Desenvolvimento do Distrito Federal e Entorno (RIDE).
'''

''
''

with st.sidebar:
    st.header("Filtros Gerais")

    dfs_para_filtrar = {
        'faixa_etaria': df_faixa_etaria,
        'cor_raca': df_cor_raca,
        'sexo': df_sexo,
        'escolaridade': df_escolaridade,
        'tprede': df_tprede,
        'acesso_internet': df_acesso_internet,
        'repositorio_inst': df_repositorio_inst,
        'uf': df_uf,
        'turnos': df_turnos,
        'concluintes': df_concluintes,
    }

    ufs = st.multiselect(
        "Unidades Federativas que Integram o RIDE:",
        options=sorted(df_faixa_etaria['UF'].unique()),
        placeholder="Escolha múltiplas UF"
    )
    if ufs:
        for nome, df in dfs_para_filtrar.items():
            if 'UF' in df.columns:
                dfs_para_filtrar[nome] = df[df["UF"].isin(ufs)]

    opcoes_ies = sorted(dfs_para_filtrar['faixa_etaria']["NO_IES"].unique())
    ies = st.multiselect(
        "Instituições de Ensino Superior:",
        options=opcoes_ies,
        placeholder="Escolha múltiplas IES"
    )
    if ies:
        for nome, df in dfs_para_filtrar.items():
            if 'NO_IES' in df.columns:
                dfs_para_filtrar[nome] = df[df["NO_IES"].isin(ies)]

    df_faixa_etaria_filtrado = dfs_para_filtrar['faixa_etaria']
    df_cor_raca_filtrado = dfs_para_filtrar['cor_raca']
    df_sexo_filtrado = dfs_para_filtrar['sexo']
    df_escol_filtrado = dfs_para_filtrar['escolaridade']
    df_tprede_filtrado = dfs_para_filtrar['tprede']
    df_acesso_internet_filtrado = dfs_para_filtrar['acesso_internet']
    df_repositorio_inst_filtrado = dfs_para_filtrar['repositorio_inst']
    df_uf_filtrado = dfs_para_filtrar['uf']
    df_turnos_filtrado = dfs_para_filtrar['turnos']
    df_concluintes_filtrado = dfs_para_filtrar['concluintes']
    
    st.markdown("---")
    st.subheader("Filtro por Número de Concluintes")

    contagens = df_concluintes_filtrado['CURSO'].value_counts()
    if not contagens.empty:
        min_concluintes = int(contagens.min())
        max_concluintes = int(contagens.max())

        if min_concluintes < max_concluintes:
            cursos_qtd_selecionada = st.slider(
                "Filtre cursos pelo nº de concluintes:",
                min_value=min_concluintes,
                max_value=max_concluintes,
                value=(min_concluintes, max_concluintes)
            )
            cursos_para_manter = contagens[
                (contagens >= cursos_qtd_selecionada[0]) & (contagens <= cursos_qtd_selecionada[1])
            ].index
            df_concluintes_filtrado = df_concluintes_filtrado[
                df_concluintes_filtrado['CURSO'].isin(cursos_para_manter)
            ]
        else:
            st.info(f"Todos os cursos na seleção têm {min_concluintes} concluintes.")

    cor_raca_conc = st.multiselect(
        "Cor e Raça",
        options=sorted(df_concluintes_filtrado["RAÇA"].unique()),
        placeholder="Escolha múltiplas Cores e Raças"
    )
    if cor_raca_conc:
        df_concluintes_filtrado = df_concluintes_filtrado[df_concluintes_filtrado['RAÇA'].isin(cor_raca_conc)]

    st.markdown("---")
    st.markdown("Filtro por Frequência de Vagas")

    contagens_turnos = df_turnos_filtrado["CURSO"].value_counts()
    if not contagens_turnos.empty:
        min_freq = int(contagens_turnos.min())
        max_freq = int(contagens_turnos.max())

        if min_freq < max_freq:
            freq_selecionada = st.slider(
                "Filtrar cursos pela frequência de turnos/ofertas:",
                min_value=min_freq,
                max_value=max_freq,
                value=(min_freq, max_freq)
            )
            cursos_para_manter_turnos = contagens_turnos[
                (contagens_turnos >= freq_selecionada[0]) & (contagens_turnos <= freq_selecionada[1])
            ].index
            df_turnos_filtrado = df_turnos_filtrado[
                df_turnos_filtrado['CURSO'].isin(cursos_para_manter_turnos)
            ]
        else:
            st.info(f"Todos os cursos na seleção têm a mesma frequência de oferta.")
    
    turnos_opt = st.multiselect(
        "Turnos",
        options=sorted(df_turnos_filtrado["TURNO"].unique()),
        placeholder="Escolha múltiplos turnos"
    )
    if turnos_opt:
        df_turnos_filtrado = df_turnos_filtrado[df_turnos_filtrado["TURNO"].isin(turnos_opt)]


frequencia_faixa_etaria, frequencia_df_faixa_etaria = pegar_frequencias(
    df_faixa_etaria_filtrado,
    "FAIXA_ETARIA",
    "Faixa Etária",
    "Frequência"
)

frequencia_cor_raca, frequencia_df_cor_raca = pegar_frequencias(
    df_cor_raca_filtrado,
    "FAIXA_ETARIA",
    "Cor_Raca",
    "Frequência"
    )

frequencia_sexo, frequencia_df_sexo = pegar_frequencias(
    df_sexo_filtrado,
    "SEXO",
    "Sexo",
    "Frequência"
)

frequencia_escol, frequencia_df_escol = pegar_frequencias(
    df_escol_filtrado,
    "ESCOLARIDADE",
    "Escolaridade",
    "Frequência"
)

frequencia_tprede, frequencia_df_tprede = pegar_frequencias(
    df_tprede_filtrado,
    "TP_REDE",
    "Tipo de rede",
    "Frequência"
)

frequencia_acesso_internet, frequencia_df_acesso_internet = pegar_frequencias(
    df_acesso_internet_filtrado,
    "IN_SERVICO_INTERNET",
    "Acesso a Internet",
    "Frequência"
)

frequencia_repositorio, frequencia_df_repositorio = pegar_frequencias(
    df_repositorio_inst_filtrado,
    "IN_REPOSITORIO_INSTITUCIONAL",
    "Repositório Acadêmico",
    "Frequência"
)

frequencia_uf, frequencia_df_uf = pegar_frequencias(
    df_uf_filtrado,
    "UF",
    "Unidade Federativa",
    "Frequência"
)

frequencia_turnos, frequencia_df_turnos = pegar_frequencias(
    df_turnos_filtrado,
    "TURNO",
    "Turno",
    "Frequência"
)

frequencia_turno_cursos, frequencia_df_turno_cursos = pegar_frequencias(
    df_turnos_filtrado,
    "CURSO",
    "Curso",
    "Frequência"
)

frequencia_concluintes, frequencia_df_concluintes = pegar_frequencias(
    df_concluintes_filtrado,
    "CURSO",
    "Curso",
    "Frequência"
)

frequencia_raca_conc, frequencia_df_raca_conc = pegar_frequencias(
    df_concluintes_filtrado,
    "RAÇA",
    "Raça",
    "Frequência"
)

frequencia_df_sexo['Percentual'] = frequencia_df_sexo['Frequência'] / frequencia_df_sexo['Frequência'].sum() * 100
frequencia_df_sexo['Percentual_str'] = frequencia_df_sexo['Percentual'].map(lambda x: f"{x:.1f}%")

frequencia_df_uf['Percentual'] = frequencia_df_uf['Frequência'] / frequencia_df_uf['Frequência'].sum() * 100
frequencia_df_uf['Percentual_str'] = frequencia_df_uf['Percentual'].map(lambda x: f"{x:.1f}%")

faixa_etaria_barra = alt.Chart(frequencia_df_faixa_etaria).mark_bar(orient='horizontal').encode(
    x=alt.X("Frequência"),
    y=alt.Y("Faixa Etária", sort='-x'),
    tooltip=["Faixa Etária", "Frequência"],
    color=alt.Color("Faixa Etária", legend=None),
).properties(
    title=alt.TitleParams(
        text="Distribuição de Docentes por Faixa Etária",
        anchor='middle',
        fontSize=20
    ),
    height=835
).interactive().configure_axisY(
    labelFontSize=15
).configure_axisX(
    labelFontSize=15
)

cor_raca_barra = alt.Chart(frequencia_df_cor_raca).mark_bar(orient='vertical').encode(
    x=alt.X("Cor_Raca", sort='-y', axis=alt.Axis(labelAngle=0)),
    y=alt.Y("Frequência"),
    tooltip=["Cor_Raca", "Frequência"],
    color=alt.Color("Cor_Raca", legend=None)
).properties(
    title=alt.TitleParams(
        text="Distribuição de Docentes por Cor e Raça",
        anchor='middle',
        fontSize=20
    ),

    width=600,
    height=400
).interactive().configure_axisY(
    labelFontSize=20
).configure_axisX(
    labelFontSize=13
)

sexo_barra = alt.Chart(frequencia_df_sexo).mark_arc(innerRadius=100).encode(
    theta=alt.Theta(field="Frequência", type="quantitative"),
    color=alt.Color(field="Sexo", type="nominal", scale=alt.Scale(
        domain=["FEM", "MASC"],
        range=["#ff69b4", "#1f77b4"]
    )),
    order=alt.Order('Sexo', sort='ascending'),
    tooltip=["Sexo", "Frequência"]
).properties(
    title=alt.TitleParams(
        text="Distribuição de Docentes por Sexo",
        anchor='middle',
        fontSize=20,
    )
).interactive()

texto_central = alt.Chart(frequencia_df_sexo).mark_text(
    text=str(frequencia_df_sexo["Frequência"].sum()),
    fontSize=50,
    fontWeight='bold',
    color='white'
).encode()

texto_fatia = alt.Chart(frequencia_df_sexo).mark_text(
    radius=120, size=14, fontWeight="bold", color="white"
).encode(
    theta=alt.Theta(field="Frequência", type="quantitative", stack="center"),
    text='Percentual_str:N',
    order=alt.Order('Sexo', sort='ascending')
)

sexo_pizza = (sexo_barra + 
              texto_central + 
              texto_fatia)


barra_escolaridade = alt.Chart(frequencia_df_escol).mark_bar(orient="vertical").encode(
    x=alt.X("Escolaridade", sort="-y", axis=alt.Axis(labelAngle=45)),
    y=alt.Y("Frequência"),
    tooltip=["Escolaridade", "Frequência"],
    color=alt.Color("Escolaridade", legend=None)
).properties(
    title=alt.TitleParams(
        text="Nivel de escolaridade dos docentes",
        anchor="middle",
        fontSize=20
    ),
    height=400
).interactive().configure_axisY(
    labelFontSize=20
).configure_axisX(
    labelFontSize=15
)

barra_tprede = alt.Chart(frequencia_df_tprede).mark_bar(orient="vertical").encode(
    x=alt.X("Tipo de rede", sort='-y', axis=alt.Axis(labelAngle=0)),
    y=alt.Y("Frequência"),
    tooltip=["Tipo de rede", "Frequência"],
    color=alt.Color("Tipo de rede", legend=None)
).properties(
    title=alt.TitleParams("Tipos de rede",
        anchor="middle",
        fontSize=20
    ),
    width=900,
    height=500
).configure_axisX(
    labelFontSize=15
).configure_axisY(
    labelFontSize=20
)

barra_acesso_internet = alt.Chart(frequencia_df_acesso_internet).mark_bar(orient="horizontal").encode(
    x=alt.X("Frequência"),
    y=alt.Y("Acesso a Internet", sort='-y'),
    tooltip=["Frequência", "Acesso a Internet"],
    color=alt.Color("Acesso a Internet", legend=None)
).properties(
    title=alt.TitleParams("Universidades com acesso a internet",
            anchor="middle")
).configure_axisX(
    labelFontSize=15
).configure_axisY(
    labelFontSize=12
)

barra_repositorio = alt.Chart(frequencia_df_repositorio).mark_bar(orient="horizontal").encode(
    x=alt.X("Frequência"),
    y=alt.Y("Repositório Acadêmico", sort="-y"),
    tooltip=["Frequência", "Repositório Acadêmico"],
    color=alt.Color("Repositório Acadêmico", legend=None)
).properties(
    title=alt.TitleParams("Universidades no repositorio",
                          anchor="middle")
).configure_axisX(
    labelFontSize=15
).configure_axisY(
    labelFontSize=12
)


pizza_uf = alt.Chart(frequencia_df_uf).mark_arc(innerRadius=100).encode(
    theta=alt.Theta(field="Frequência", type="quantitative"),
    color=alt.Color(field="Unidade Federativa", type="nominal", scale=alt.Scale(
        domain=["DF", "GO", "MG"],
        range=['#4c78a8', '#f58518', '#e45756']))
).properties(
    title=alt.TitleParams("Universidades por Unidade Federativa",
                          anchor="middle")
)

texto_central_uf = alt.Chart(frequencia_df_uf).mark_text(
    text=f'{frequencia_df_uf["Frequência"].sum()}',
    fontSize=50,
    fontWeight='bold',
    color='white'
).encode()

texto_fatia_uf = alt.Chart(frequencia_df_uf).mark_text(
    radius=120, size=14, fontWeight="bold", color="white"
).encode(
    theta=alt.Theta(field="Frequência", type="quantitative", stack="center"),
    text='Percentual_str:N',
    order=alt.Order('Frequência', sort='descending')
)

grafico_final_uf = (pizza_uf 
                    + texto_central_uf 
                    + texto_fatia_uf)

vagas_barra = alt.Chart(frequencia_df_turnos).mark_bar(orient="horizontal").encode(
    x=alt.X("Frequência"),
    y=alt.Y("Turno", sort="-x"),
    tooltip=["Frequência", "Turno"],
    color=alt.Color("Turno", legend=None)
)

concluintes_bar = alt.Chart(frequencia_df_concluintes).mark_bar(orient="vertical").encode(
    x=alt.X("Curso", sort="-y", axis=alt.Axis(labelAngle=45)),
    y=alt.Y("Frequência"),
    tooltip=["Curso", "Frequência"],
    color=alt.Color("Curso", legend=None)
).properties(
    title=alt.TitleParams("Total de concluintes por curso",
                          anchor="middle")
).configure_axisX(
    labelFontSize=15
).configure_axisY(
    labelFontSize=15
)

concluintes_cor_raca_bar = alt.Chart(frequencia_df_raca_conc).mark_bar(orient="horizontal").encode(
    x=alt.X("Frequência"),
    y=alt.Y("Raça", sort="-x"),
    tooltip=["Raça", "Frequência"],
    color=alt.Color("Raça", legend=None)
).properties(
    title=alt.TitleParams("Total de concluintes por cor e raça",
                          anchor="middle")
).configure_axisX(
    labelFontSize=15
).configure_axisY(
    labelFontSize=12
)

turnos_bar = alt.Chart(frequencia_df_turnos).mark_bar(orient="horizontal").encode(
    x=alt.X("Frequência"),
    y=alt.Y("Turno", sort="-x"),
    tooltip=["Frequência", "Turno"],
    color=alt.Color("Turno", legend=None)
).properties(
    title=alt.TitleParams("Total de vagas disponíveis por turno",
                          anchor="middle")
).configure_axisX(
    labelFontSize=15
).configure_axisY(
    labelFontSize=12
)

turnos_cursos_bar = alt.Chart(frequencia_df_turno_cursos).mark_bar(orient="vertical").encode(
    x=alt.X("Curso", sort="-y", axis=alt.Axis(labelAngle=45)),
    y=alt.Y("Frequência"),
    tooltip=["Curso", "Frequência"],
    color=alt.Color("Curso", legend=None)
).properties(
    title=alt.TitleParams("Total de vagas disponíveis por curso",
                          anchor="middle")
).configure_axisX(
    labelFontSize=15
).configure_axisY(
    labelFontSize=15
)

df_agg = df_escol_cor.groupby(['ESCOLARIDADE', 'COR_RACA']).size().reset_index(name='quantidade')

escol_cor_tree = alt.Chart(df_agg).mark_bar().encode(
    x=alt.X('ESCOLARIDADE:N', title='Escolaridade', sort=None, axis=alt.Axis(labelAngle=0)),
    y=alt.Y('sum(quantidade):Q',
            axis=alt.Axis(title='Proporção de Docentes', format='%'),
            stack='normalize'),

    color=alt.Color('COR_RACA:N', title='Cor/Raça'),

    tooltip=[
        alt.Tooltip('ESCOLARIDADE:N', title='Escolaridade'),
        alt.Tooltip('COR_RACA:N', title='Cor/Raça'),
        alt.Tooltip('sum(quantidade):Q', title='Quantidade')
    ]
).properties(
    title=alt.TitleParams("Composição de Cor/Raça por Nível de Escolaridade",
        anchor="middle"),
    width=600,
    height=800
).configure_axisX(
    labelFontSize=15
).configure_axisY(
    labelFontSize=15
)

df_acesso_rede = pd.merge(df_acesso_internet_filtrado, df_tprede_filtrado, on=['NO_IES', 'UF'])

base_acesso_rede = alt.Chart(df_acesso_rede).encode(
    alt.X('TP_REDE:N', title='Tipo de Rede'),
    alt.Y('UF:N', title='UF')
).properties(
    width=250,
    height=200
)

heatmap_layer = base_acesso_rede.mark_rect().encode(
    alt.Color('count():Q',
              scale=alt.Scale(scheme='viridis'),
              title='Nº de Instituições')
)

text_layer = base_acesso_rede.mark_text(baseline='middle').encode(
    text=alt.Text('count():Q'),
    color=alt.value('black')
)

df_acesso_rede_final = (heatmap_layer + text_layer).facet(
    column=alt.Column('IN_SERVICO_INTERNET:N', title='Possui acesso à internet?')
).properties(
    title='Distribuição de Instituições por UF que possui ou não acesso à internet'
).configure_axisX(
    labelFontSize=15
).configure_axisY(
    labelFontSize=15
).interactive()

geojson_url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
geojson = requests.get(geojson_url).json()

df_tabela_mapa = pd.DataFrame({
    'estado': ['DF', 'GO', 'MG', 'SP'],
    'valor': [61, 18, 3, 0]
})

gdf = gpd.GeoDataFrame.from_features(geojson["features"])
gdf = gdf.set_index('sigla')

gdf['valor'] = df_tabela_mapa.set_index('estado')['valor']
gdf['valor'] = gdf['valor'].fillna(0)

gdf['centroide'] = gdf.geometry.centroid
gdf['lat'] = gdf['centroide'].y
gdf['lon'] = gdf['centroide'].x

choropleth = go.Choropleth(
    geojson=geojson,
    locations=gdf.index,
    z=[v if v > 1 else None for v in gdf['valor']],
    featureidkey="properties.sigla",
    colorscale="Viridis",
    marker_line_color='white',
    marker_line_width=0.5,
    colorbar_title="Valor"
)

text_annotations = go.Scattergeo(
    lon=gdf.loc[gdf['valor'] > 1, 'lon'],
    lat=gdf.loc[gdf['valor'] > 1, 'lat'],
    text=gdf.loc[gdf['valor'] > 1].index,
    mode='text',
    textfont=dict(
        size=14,
        color='black',
        family='Tahoma'
    )
)

fig = go.Figure(data=[choropleth, text_annotations])

fig.update_geos(
    fitbounds="locations",
    visible=False
)

fig.update_layout(
    geo=dict(
        bgcolor='rgba(0,0,0,0)'
    ),
    paper_bgcolor='rgba(0,0,0,0)',
    margin={"r":0,"t":0,"l":0,"b":0}
)


aba1, aba2, aba3, aba4, aba5 = st.tabs(["Docentes", "Formados", "Redes", "Concluintes", "Vagas"])

with aba1:
    col_esquerda, col_direita = st.columns([1, 2], gap="large")
    with col_esquerda:
        st.altair_chart(sexo_pizza, use_container_width=True)
        st.markdown("---")
        st.altair_chart(barra_escolaridade, use_container_width=True)
        
        
    with col_direita:
        st.altair_chart(faixa_etaria_barra, use_container_width=True)

    st.altair_chart(cor_raca_barra, use_container_width=True)

with aba2:
    st.altair_chart(escol_cor_tree, use_container_width=True)
    
with aba3:
    col1, col2, col3 = st.columns(3, gap="large")

    with col1:
        st.altair_chart(barra_acesso_internet, use_container_width=True)
        st.altair_chart(barra_tprede, use_container_width=True)

    with col2:
        st.plotly_chart(fig, use_container_width=True)
        st.altair_chart(df_acesso_rede_final)
        
    with col3:
        st.altair_chart(barra_repositorio, use_container_width=True)

with aba4:
    st.altair_chart(concluintes_cor_raca_bar)
    st.altair_chart(concluintes_bar, use_container_width=True)

with aba5:
    st.altair_chart(turnos_bar, use_container_width=True)
    st.altair_chart(turnos_cursos_bar, use_container_width=True)
