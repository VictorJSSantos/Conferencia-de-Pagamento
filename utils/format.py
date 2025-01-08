import numpy as np


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
