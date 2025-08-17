"""Dashboard Streamlit para o Índice de Receptividade do Mercado

Carrega `src/processed_data/indicie_de_receptividade_do_mercado.csv` e permite ao usuário
customizar pesos e visualizar gráficos interativos.
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


@st.cache_data
def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";")
    # garantir tipos
    df["ano"] = df["ano"].astype(str)
    # colunas numéricas
    for col in [
        "empregabilidade",
        "demanda",
        "salario_mediana",
        "demanda_normalizada",
        "salario_mediana_normalizado",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def compute_index(
    df: pd.DataFrame, w_emp: float, w_dem: float, w_sal: float, use_normalized: bool
) -> pd.DataFrame:
    df = df.copy()
    if (
        use_normalized
        and "demanda_normalizada" in df.columns
        and "salario_mediana_normalizado" in df.columns
    ):
        dem_col = "demanda_normalizada"
        sal_col = "salario_mediana_normalizado"
    else:
        dem_col = "demanda"
        sal_col = "salario_mediana"

    # normalização simples para colocar tudo na mesma escala se colunas não normalizadas forem usadas
    if dem_col == "demanda" or sal_col == "salario_mediana":
        # normaliza demanda e salario por min-max por ano
        df["demanda_norm_tmp"] = df.groupby("ano")[dem_col].transform(
            lambda x: (x - x.min()) / (x.max() - x.min()) if x.max() != x.min() else 0
        )
        df["salario_norm_tmp"] = df.groupby("ano")[sal_col].transform(
            lambda x: (x - x.min()) / (x.max() - x.min()) if x.max() != x.min() else 0
        )
        dem_used = "demanda_norm_tmp"
        sal_used = "salario_norm_tmp"
    else:
        dem_used = dem_col
        sal_used = sal_col

    df["indice_receptividade"] = (
        w_emp * df["empregabilidade"].fillna(0)
        + w_dem * df[dem_used].fillna(0)
        + w_sal * df[sal_used].fillna(0)
    )
    return df


def main():
    st.set_page_config(page_title="Índice de Receptividade do Mercado", layout="wide")
    st.title("Dashboard — Índice de Receptividade do Mercado")

    data_path = (
        Path(__file__).parent
        / "processed_data"
        / "indicie_de_receptividade_do_mercado.csv"
    )
    if not data_path.exists():
        st.error(f"Arquivo não encontrado: {data_path}")
        return

    df = load_data(data_path)

    # Sidebar — filtros e pesos
    st.sidebar.header("Parâmetros")
    years = sorted(df["ano"].unique())
    sel_year = st.sidebar.selectbox("Ano", options=years, index=len(years) - 1)
    setores = sorted(df["setor"].unique())
    sel_setor = st.sidebar.multiselect(
        "Setor (filtrar)", options=setores, default=setores
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Pesos")
    w_emp = st.sidebar.slider("Peso: Empregabilidade (x)", 0.0, 1.0, 0.33, 0.01)
    w_dem = st.sidebar.slider("Peso: Demanda (y)", 0.0, 1.0, 0.33, 0.01)
    w_sal = st.sidebar.slider("Peso: Remuneração (z)", 0.0, 1.0, 0.34, 0.01)

    st.sidebar.markdown("---")
    st.sidebar.write("Soma dos pesos: ", round(w_emp + w_dem + w_sal, 3))

    # Filtrar dados
    df_filtered = df[df["ano"].isin([sel_year])] if sel_setor != [] else df
    df_filtered = df_filtered[df_filtered["setor"].isin(sel_setor)]

    df_indexed = compute_index(df, w_emp, w_dem, w_sal, True)

    # Layout dos gráficos
    st.markdown("## Visão Geral")
    col1, col2 = st.columns((2, 1))

    # Gráfico 1: série temporal do índice por setor
    with col1:
        st.subheader("Evolução do Índice por Setor")
        fig_line = px.line(
            df_indexed[df_indexed["setor"].isin(sel_setor)],
            x="ano",
            y="indice_receptividade",
            color="setor",
            markers=True,
            labels={"indice_receptividade": "Índice de Receptividade", "ano": "Ano"},
        )
        st.plotly_chart(fig_line, use_container_width=True)

    with col2:
        st.subheader(f"Ranking — Ano {sel_year}")
        df_year = df_indexed[df_indexed["ano"] == sel_year].sort_values(
            "indice_receptividade", ascending=False
        )
        st.dataframe(
            df_year[
                [
                    "setor",
                    "empregabilidade",
                    "demanda",
                    "salario_mediana",
                    "indice_receptividade",
                ]
            ].reset_index(drop=True)
        )

    st.markdown("---")

    # Gráfico 2: barras por setor no ano selecionado
    st.subheader("Índice por Setor (ano selecionado)")
    fig_bar = px.bar(
        df_year,
        x="setor",
        y="indice_receptividade",
        color="setor",
        labels={"indice_receptividade": "Índice"},
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # Heatmap de índice (setor x ano)
    st.subheader("Heatmap: Índice por Setor e Ano")
    pivot = df_indexed.pivot_table(
        index="setor", columns="ano", values="indice_receptividade"
    )
    fig_heat = px.imshow(
        pivot.fillna(0), labels=dict(x="Ano", y="Setor", color="Índice")
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    st.markdown("---")
    st.subheader("Dados brutos e download")
    st.write("Visualize e faça download dos dados com o índice calculado.")
    st.dataframe(df_indexed)
    csv = df_indexed.to_csv(sep=";", index=False)
    st.download_button(
        "Baixar CSV com índice",
        data=csv,
        file_name="indice_receptividade_computado.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    main()
