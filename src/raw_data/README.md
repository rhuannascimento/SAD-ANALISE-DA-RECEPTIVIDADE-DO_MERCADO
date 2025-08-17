# raw_data — arquivos de entrada (RAIS)

Esta pasta armazena os arquivos de dados brutos usados pela pipeline de processamento.

Os dados de entrada (originais) podem ser obtidos no link abaixo:

https://drive.google.com/drive/folders/1yGt61tLFxRr1x1p5dQDj0mPsU9TacCm6?usp=sharing

Por favor, baixe os arquivos necessários e coloque-os em `src/raw_data/` (ou aponte os scripts para o local onde você salvar os dados).

Arquivos esperados (exemplos):

- `rais-combinado.csv`
	- CSV principal com dados RAIS combinados.
	- Separador esperado: `;` (pode variar — os scripts tentam detectar colunas mesmo com pequenas variações).
	- Encoding: preferencialmente UTF-8; se não funcionar, os scripts tentam `latin-1`.
	- Colunas úteis (nomes podem variar): `Ano`, `ID CNAE`, `CNAE`, `Massa Salarial`, `Salário Médio`, `Número de empregos`, `Ganho de Oportunidade`.

- `rais_com_cnaes_setor.csv`
	- Produto gerado pelo script `merge_rais_cnaes.py` juntando `rais-combinado.csv` com `cnaes_unicos.csv` e adicionando a coluna `SETOR`.
