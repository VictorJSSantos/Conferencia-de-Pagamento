from ast import literal_eval
from datetime import datetime
import numpy as np
import pandas as pd


def corrigir_separadores(valor):
    """
    Função responsável por corrigir separadores de milhar e decimais
    em números representados como strings.

    Parâmetros:
    valor (str): Número em formato de string com separadores incorretos.

    Retorna:
    float: Valor corrigido como número decimal.
    np.nan: Caso o valor não seja conversível.
    """
    try:
        if (
            isinstance(valor, str)
            and "," in valor
            and float(valor.replace(",", ".")) < 1
        ):
            valor = valor.replace(".", "").replace(",", ".")
        return float(valor)
    except ValueError:
        return np.nan


# 1Função para converter a string em uma lista de dicionários
def converter_lista(string):
    try:
        return literal_eval(string)
    except (ValueError, SyntaxError):
        return []


# Função para obter a data atual no formato AAAA-MM-DD
def obter_data_atual():
    return datetime.now().strftime("%Y-%m-%d")


def expandir_dicionario_para_linhas(df, coluna, chave_id, chave_pedido_id):
    # Cria uma lista para armazenar os DataFrames temporários de cada dicionário
    dfs_temporarios = []

    # Itera sobre cada linha do DataFrame original
    for index, row in df.iterrows():
        # Obtém a lista de dicionários da coluna especificada
        lista_dicionarios = row[coluna]
        # Obtém o pedido_id da linha
        pedido_id = row[chave_pedido_id]
        # Itera sobre cada dicionário na lista
        for dicionario in lista_dicionarios:
            # Adiciona o pedido_id ao dicionário
            dicionario[chave_pedido_id] = pedido_id
            # Obtém o valor do ID do dicionário
            id_valor = dicionario[chave_id]
            # Remove o ID do dicionário
            # del dicionario[chave_id]
            # Cria um DataFrame temporário com o dicionário como uma única linha
            df_temporario = pd.DataFrame(dicionario, index=[0])
            # Adiciona o valor do ID como uma nova coluna
            df_temporario[chave_id] = id_valor
            # Adiciona o DataFrame temporário à lista
            dfs_temporarios.append(df_temporario)
    # Concatena todos os DataFrames temporários em um único DataFrame
    df_resultado = pd.concat(dfs_temporarios, ignore_index=True)

    return df_resultado


def expandir_dicionario_para_colunas(df, coluna):
    # Cria um DataFrame temporário com os dicionários expandidos
    df_temporario = pd.DataFrame(df[coluna].tolist())

    # Adiciona um prefixo para as novas colunas com o nome da coluna original
    df_temporario.columns = [f"{coluna}_{col}" for col in df_temporario.columns]

    # Concatena o DataFrame principal com o DataFrame temporário
    df = pd.concat([df, df_temporario], axis=1)

    return df


def expandir_dicionario_para_colunas_tratado(df, coluna):
    # Substituir valores nulos ou inválidos por dicionários vazios
    df[coluna] = df[coluna].apply(lambda x: x if isinstance(x, (dict, list)) else {})

    # Converter a coluna em um DataFrame expandido
    df_temporario = pd.DataFrame(df[coluna].tolist())

    # Adiciona um prefixo para as novas colunas com o nome da coluna original
    df_temporario.columns = [f"{coluna}_{col}" for col in df_temporario.columns]

    # Concatena o DataFrame principal com o DataFrame temporário
    df = pd.concat([df, df_temporario], axis=1)


def get_cep_number(cep):
    if cep is not None:
        cep = cep.replace("-", "")
        cep = cep.replace(".", "")
    else:
        cep = None

    return cep


# Função para encontrar o intervalo de CEP correspondente e retornar 'Geografia Comercial'
def encontrar_geografia_comercial(cep, df_b):
    if pd.isna(cep):
        return None
    else:
        for idx, intervalo in enumerate(df_b.index):
            if intervalo.left <= cep <= intervalo.right:
                return df_b.at[intervalo, "Geografia Comercial"]
        return None


def encontrar_informacoes_tabela_abrangencia(cep, df_b, cols):
    """
    Retorna os valores das colunas especificadas, com base no intervalo de CEP correspondente.

    Args:
        cep (int or float): O CEP a ser buscado.
        df_b (DataFrame): DataFrame com um índice de intervalos e colunas de interesse.
        cols (str or list): Nome da coluna ou lista de colunas a serem retornadas.

    Returns:
        dict or None: Dicionário com as colunas e valores correspondentes, ou None se não encontrado.
    """
    if pd.isna(cep):
        return None

    # Garantir que `cols` seja uma lista para processamento uniforme
    if isinstance(cols, str):
        cols = [cols]

    # Procurar o intervalo correspondente
    for intervalo in df_b.index:
        if intervalo.left <= cep <= intervalo.right:
            # Retornar os valores das colunas especificadas
            return {col: df_b.at[intervalo, col] for col in cols}

    # Caso nenhum intervalo corresponda
    return None


# Função para consultar o valor em df_c com base em 'Geografia Comercial' e 'metodo_pesagem'
def consultar_valor_geografia_comercial_metodo_pesagem(row, df_c):
    categoria_peso = row["categoria_peso"]
    geografia_comercial = row["Geografia_Comercial_tbAbran"]
    # print(f'Método de Pesagem: {metodo_pesagem}, Geografia Comercial: {geografia_comercial}')
    if categoria_peso in df_c.index and geografia_comercial in df_c.columns:
        # print(f'Valor encontrado em df_c: {df_c.at[metodo_pesagem, geografia_comercial]}')
        return df_c.at[categoria_peso, geografia_comercial]
    else:
        # print('Valores não encontrados em df_c')
        # print(f'metodo_pesagem in df_c.index: {categoria_peso in df_c.index}')
        # print(f'geografia_comercial in df_c.columns: {geografia_comercial in df_c.columns}')
        return None
