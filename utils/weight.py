def adicionar_peso_caixa(peso):
    """
    Função responsável por adicionar o peso da caixa ao peso atual, de acordo com a heurística.

    Params:
    :peso: O peso do pacote enviado, que determinará o peso da caixa (correspondente ao peso)

    Return:
    :peso da caixa: Peso da caixa para este pedido
    """

    if peso >= 5:
        return 0.450
    if peso >= 3:
        return 0.300
    else:
        return 0.200


def calcular_peso_cubado(peso):
    """
    Função responsável por calcular o peso cubado do pedido, aplicando o fator cubico.

    Params:
    :peso: O peso do pacote enviado, que determinará as dimensões da caixa (correspondente ao peso).

    Return:
    :peso cubado: Peso cubado da encomenda
    """
    if peso >= 5:
        comprimento, largura, altura, fator_cubico = 0.350, 0.290, 0.140, 167
        peso_cubado = peso * comprimento * largura * altura * fator_cubico
        return peso_cubado
    if peso >= 3:
        comprimento, largura, altura, fator_cubico = 0.220, 0.210, 0.80, 167
        peso_cubado = peso * comprimento * largura * altura * fator_cubico
        return peso_cubado
    else:
        comprimento, largura, altura, fator_cubico = 0.180, 0.130, 0.80, 167
        peso_cubado = peso * comprimento * largura * altura * fator_cubico
        return peso_cubado


def escolha_metodo_pesagem(df):
    """
    Função responsável por fazer a heurística se será o peso cubado ou peso real da encomenda a ser cobrado

    Params:
    :peso: O peso do pacote enviado, que determinará o peso da caixa (correspondente ao peso)

    Return:
    :classificacao: Classificação entre peso cubado ou peso real.
    """
    peso = df[0]
    # peso_cubado = df[1]

    if peso >= 5:
        return "peso_cubado"
    else:
        return "peso"


def gerar_intervalos():
    """
    Generador de intervalos de peso para adequação da cobrança.

    Params:
    :peso: O peso do pacote enviado, que determinará o peso da caixa (correspondente ao peso)

    Return:
    :generator: Gerador de intervalores para ser utilizado nos intervalos de peso.
    """
    peso = 30
    row_peso = 33
    while peso > 0.25:
        yield peso, row_peso
        if peso > 1:
            peso -= 1  # Intervalos de 1 para pesos maiores que 1
        else:
            peso -= 0.25  # Intervalos de 0.25 para pesos menores ou iguais a 1
        row_peso -= 1
    yield 0.25, 0  # Caso especial: peso <= 0.25


def intervalo_peso(peso):
    """
    Função que encontra o intervalo de peso da encomenda considerando
    as faixas de peso cobradas pelo parceiro de transporte.

    Params:
    :peso: O peso do pacote enviado.

    Return:
    :intervalo de peso: Linha correspondente da faixa de peso da
                        encomenda na tabela de preço do parceiro de Transporte.
    """
    if peso > 30:
        adicional = peso - int(peso)  # Obtém a parte decimal do peso
        row_adicional = 34
        row_peso = 33
        return row_peso, row_adicional, adicional

    for limite, row_peso in gerar_intervalos():
        if peso > limite:
            return row_peso

    return 0  # Caso peso seja menor ou igual a 0.25
