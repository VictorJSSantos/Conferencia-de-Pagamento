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
        dict: Dicionário com as colunas e valores correspondentes (ou valores vazios, se não encontrado).
    """
    if pd.isna(cep):
        return {col: None for col in cols}

    # Garantir que `cols` seja uma lista para processamento uniforme
    if isinstance(cols, str):
        cols = [cols]

    # Procurar o intervalo correspondente
    for intervalo in df_b.index:
        if intervalo.left <= cep <= intervalo.right:
            return {col: df_b.at[intervalo, col] for col in cols}

    # Retornar valores vazios caso nenhum intervalo corresponda
    return {col: None for col in cols}


# Função para consultar o valor em df_c com base em 'Geografia Comercial' e 'metodo_pesagem'
def consultar_valor_geografia_comercial_metodo_pesagem(row, df_c):
    categoria_peso = row["categoria_peso"]
    geografia_comercial = row["Geografia_Comercial_tbAbran"]
    # print(f'Método de Pesagem: {metodo_pesagem}, Geografia Comercial: {geografia_comercial}')
    if categoria_peso in df_c.index and geografia_comercial in df_c.columns:
        # print(f'Valor encontrado em df_c: {df_c.at[metodo_pesagem, geografia_comercial]}')
        return df_c.at[categoria_peso + 1, geografia_comercial]
    ###########################################################################################
    ################### AQUI ADICONEI O + 1 PARA VERIFICAR SE A LINHA SE AJEITA ###############
    ###########################################################################################
    else:
        # print('Valores não encontrados em df_c')
        # print(f'metodo_pesagem in df_c.index: {categoria_peso in df_c.index}')
        # print(f'geografia_comercial in df_c.columns: {geografia_comercial in df_c.columns}')
        return None


# Aplicando Transformações para Ter custos de GRIS
def calcular_custo_gris(risco, valor_nf):
    """
    Calcula o Custo de GRIS com base no risco e no valor da nota fiscal.

    Args:
        risco (str): O nível de risco ('Normal', 'Alto Risco', 'Altíssimo Risco').
        valor_nf (float): O valor da nota fiscal.

    Returns:
        float: O custo de GRIS calculado.
    """
    if pd.isna(valor_nf) or pd.isna(risco):
        return None

    # Mapeamento de risco para percentuais
    percentual_gris = {
        "Padrão": 0.002,  # 0,2%
        "Alto Risco": 0.01,  # 1%
        "Altíssimo Risco": 0.02,  # 2%
    }

    return valor_nf * percentual_gris.get(
        risco, 0
    )  # Retorna 0 se o risco não estiver no dicionário


def calcular_custo_seguro(valor_nf):
    if pd.isna(valor_nf):
        return None

    percentual_seguro = 0.004
    return percentual_seguro * valor_nf


def calcular_valor_a_ser_pago_total(
    valor_frete_peso, valor_seguro, valor_gris, aliquota_icms
):
    total = valor_frete_peso + valor_seguro + valor_gris
    # aliquota_icms = int(aliquota_icms)
    aliquota_icms = 1 - (aliquota_icms / 100)
    total = total / aliquota_icms
    return total
