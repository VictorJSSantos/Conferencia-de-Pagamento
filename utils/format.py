from ast import literal_eval
from datetime import datetime
import numpy as np
import pandas as pd


# Função para corrigir os separadores apenas para valores inválidos
def corrigir_separadores(valor):
    """
    Função responsável por tornar os números em padrão internacional,
    ou seja, com separadores de milhar "." e separadores de centavos ",".

    Params:
    :valor: Número a ser transformado

    Return:
    :generator: Gerador de intervalores para ser utilizado nos intervalos de peso.
    """

    try:
        # Verificar se é menor que 1 e contém ',' como separador decimal
        if (
            isinstance(valor, str)
            and "," in valor
            and float(valor.replace(",", ".")) < 1
        ):
            valor = valor.replace(".", "").replace(
                ",", "."
            )  # Corrigir milhar e decimal
        return float(valor)
    except ValueError:
        return np.nan  # Retornar NaN se não for conversível


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
