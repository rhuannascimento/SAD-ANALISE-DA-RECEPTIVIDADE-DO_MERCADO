
# Projeto Sistema de apoio à decisão — Análise da Receptividade do Mercado de Trabalho Brasileiro

Este repositório contém uma pequena pipeline de processamento de dados construída em Python para agregar e enriquecer dados da RAIS com classificação por setor (CNAE), calcular métricas como empregabilidade e salário mediano por setor e normalizar indicadores para uso posterior.

## Visão geral

Objetivo: transformar os dados brutos da RAIS em indicadores por ano e setor com baixo uso de memória, usando operações em streaming quando necessário.

Principais artefatos:
- Scripts em `src/scripts/` para cada etapa do processamento.
- Dados brutos em `src/raw_data/`.
- Dados processados em `src/processed_data/`.

## Estrutura do repositório

- `requirements.txt` — dependências do projeto (ex.: pandas).
- `src/scripts/` — scripts de processamento (ver detalhes abaixo):
	- `extrair_cnaes.py` — extrai CNAEs únicos do CSV RAIS.
	- `merge_rais_cnaes.py` — faz join streaming entre RAIS e CNAEs, adicionando coluna `SETOR`.
	- `compute_empregabilidade.py` — agrega empregos por ano+setor e calcula empregabilidade usando taxas de `desocupacao.json`.
	- `compute_salario_medio_setor.py` — agrega salário médio/mediana por ano+setor.
	- `normalize_salario_demanda.py` — normaliza demanda e salário por setor para o intervalo [0,1].
- `src/raw_data/` — arquivos de entrada.
- `src/processed_data/` — arquivos gerados pela pipeline.

> Observação: caminhos relativos usados pelos scripts esperam os arquivos em `data/` ou conforme argumento CLI; ver exemplos de uso.

## Requisitos

O projeto usa Python 3.8+ (compatível com versões mais recentes). As dependências mínimas estão em `requirements.txt`.

Instalação (ambiente virtual recomendado):

```bash
python -m venv .venv
source .venv/bin/activate  # no Windows com bash: .venv/Scripts/activate
pip install -r requirements.txt
```

## Fluxo de processamento (exemplo)

1. Extrair CNAEs únicos (gera `cnaes_unicos.csv`):

```bash
python src/scripts/extrair_cnaes.py
```

Por padrão `extrair_cnaes.py` espera `./data/rais-combinado.csv` e escreve `cnaes_unicos.csv` no diretório de trabalho atual. Você pode editar o script ou adaptar o caminho se necessário.

2. Juntar RAIS com os CNAEs e atribuir `SETOR` (streaming):

```bash
python src/scripts/merge_rais_cnaes.py --rais src/raw_data/rais-combinado.csv --cnaes src/processed_data/cnaes_unicos.csv --out src/raw_data/rais_com_cnaes_setor.csv
```

3. Calcular empregabilidade por ano e setor (usa `desocupacao.json`):

```bash
python src/scripts/compute_empregabilidade.py --rais src/raw_data/rais_com_cnaes_setor.csv --desemp src/processed_data/desocupacao.json --out src/processed_data/empregabilidade_por_setor.csv
```

4. Calcular salário mediano/médio por ano e setor:

```bash
python src/scripts/compute_salario_medio_setor.py --rais src/raw_data/rais_com_cnaes_setor.csv --out src/processed_data/salario_medio_por_setor.csv
```

5. Normalizar demanda e salário por setor (gera colunas `demanda_normalizada` e `salario_mediana_normalizado`):

```bash
python src/scripts/normalize_salario_demanda.py --in src/processed_data/salario_medio_por_setor.csv --out src/processed_data/salario_medio_por_setor_normalizado.csv
```

Observação: os scripts aceitam argumentos CLI (veja `--help` em cada script) e são escritos para tolerar variações de encoding (UTF-8 / latin-1) e formatos numéricos (vírgula/ponto).

## Descrição curta dos scripts

- `extrair_cnaes.py`:
	- Entrada: CSV RAIS (esperado `rais-combinado.csv`, separador `;`).
	- Saída: `cnaes_unicos.csv` (contém `ID CNAE` e `CNAE`) — remove duplicatas por `ID CNAE`.

- `merge_rais_cnaes.py`:
	- Faz merge streaming entre RAIS e `cnaes_unicos.csv` por `ID CNAE`.
	- Gera colunas ordenadas e adiciona `SETOR` ao output.

- `compute_empregabilidade.py`:
	- Agrega número de empregos por (ano, setor) e aplica taxa de desocupação (arquivo JSON) para calcular empregabilidade.
	- Fórmula usada: empregabilidade = empregados / (empregados + desempregados), com desempregados estimados a partir da taxa.

- `compute_salario_medio_setor.py`:
	- Agrega salários por (ano, setor) e computa a mediana (por defeito ignore zeros; pode incluir zeros com flag `--include-zeros`).

- `normalize_salario_demanda.py`:
	- Normaliza duas colunas (demanda e salário mediana) para [0,1] por setor usando min-max calculado dentro de cada setor.

## Formatos de arquivo

- CSVs de entrada/saída usam separador `;` na maior parte dos scripts (exceto `cnaes_unicos.csv` que historicamente é `,`).
- JSON: `desocupacao.json` deve mapear ano -> taxa (ex.: {"2019": 11.5}).

## Boas práticas e observações

- Os scripts foram desenvolvidos para processar arquivos grandes com baixo uso de memória: leitura em streaming e agregação por chaves (ano,setor).
- Os identificadores das colunas são detectados com tolerância (nomes com/sem acentos, variações maiúsculas/minúsculas).
- Números aceitam formatos com `.` e `,` (milhar/decimal) e tentam recuperar o valor quando possível.
