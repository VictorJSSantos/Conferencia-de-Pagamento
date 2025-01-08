import json
import requests
import sys
from tqdm import tqdm

from blingapi.nfes import *
from blingapi.logistica import *

from hostgatorapi.orders import *


from utils.configs import *
from utils.format import *
from utils.request import *
from utils.weight import *


if __name__ == "__main__":
    # Chaves esperadas no JSON de autorização
    TOKENS = [
        "DLOJAVIRTUAL_KEY",
        "DLOJAVIRTUAL_URL",
        "BLING_CLIENT_ID",
        "BLING_CLIENT_SECRET",
        "BLING_REDIRECT_URI",
        "access_token",
        "expires_in",
        "token_type",
        "scope",
        "refresh_token",
    ]

    TOKENS_DICT = load_tokens(TOKENS)

    # HOSTGATOR TOKENS
    DLOJAVIRTUAL_KEY = TOKENS_DICT["DLOJAVIRTUAL_KEY"]
    DLOJAVIRTUAL_URL = TOKENS_DICT["DLOJAVIRTUAL_URL"]

    # BLING TOKENS
    BLING_CLIENT_ID = TOKENS_DICT["BLING_CLIENT_ID"]
    BLING_CLIENT_SECRET = TOKENS_DICT["BLING_CLIENT_SECRET"]
    BLING_REDIRECT_URI = TOKENS_DICT["BLING_REDIRECT_URI"]
    ACCESS_TOKEN = TOKENS_DICT["access_token"]
    EXPIRES_IN = TOKENS_DICT["expires_in"]
    TOKEN_TYPE = TOKENS_DICT["token_type"]
    SCOPE = TOKENS_DICT["scope"]
    REFRESH_TOKEN = TOKENS_DICT["refresh_token"]

    try:

        if not TOKENS:
            print("Token de acesso não disponível")

        url_validator = "https://www.bling.com.br/Api/v3/logisticas"
        if not ACCESS_TOKEN:
            print("Access token não disponível")

        headers = {
            "Authorization": f"Bearer {ACCESS_TOKEN}",
            "Accept": "application/json",
        }

        params = {"page": 1, "limit": 100}

        response = requests.get(url_validator, headers=headers, params=params)
        print(response.text)
        if response.status_code != 200:

            raise Exception()

    except:
        invite_url = input("Insira a URL de convite: ")
        state = invite_url[-32:]

        # URL para obtenção do authorization code
        authorization_url = "https://www.bling.com.br/Api/v3/oauth/authorize"
        token_url = "https://www.bling.com.br/Api/v3/oauth/token"

        # Parâmetros para a obtenção do authorization code
        authorization_params = {
            "response_type": "code",
            "client_id": BLING_CLIENT_ID,
            "redirect_uri": BLING_REDIRECT_URI,
            "state": state,
        }

        # Construindo a URL para obtenção do authorization code
        authorization_url_with_params = (
            f"{authorization_url}?{urlencode(authorization_params)}"
        )

        print(
            f"Por favor, visite a seguinte URL e autorize o aplicativo:\n{authorization_url_with_params}"
        )

        # Após autorização, insira o código retornado
        authorization_code = input("Insira o código de autorização: ")

        # Solicitação de tokens de acesso
        token_data = {
            "grant_type": "authorization_code",
            "code": authorization_code,
            "client_id": BLING_CLIENT_ID,
            "client_secret": BLING_CLIENT_SECRET,
            "redirect_uri": BLING_REDIRECT_URI,
        }

        auth_headers = {
            "Authorization": f'Basic {base64.b64encode(f"{BLING_CLIENT_ID}:{BLING_CLIENT_SECRET}".encode()).decode()}',
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "1.0",
        }

        # Solicitação de tokens de acesso com as credenciais do cliente no header
        response = requests.post(token_url, data=token_data, headers=auth_headers)

        if response.status_code == 200:
            # Tokens obtidos com sucesso
            tokens = response.json()
            access_token = tokens.get("access_token")
            refresh_token = tokens.get("refresh_token")

            print(f"Access Token: {access_token}")
            print(f"Refresh Token: {refresh_token}")
            print(f"AGORA VAMOS VER O QUE É {tokens}")
            save_tokens(tokens)
        else:
            # Tratamento de erro
            print(
                f"Erro na obtenção dos tokens. Código de status: {response.status_code}"
            )
            print(response.text)

    finally:
        if response.status_code == 200:
            print("Podemos prosseguir!")
            pass
        else:
            print("Algum erro aconteceu, tente novamente!")
            quit = input("Aperte qualquer tecla para fechar o prograa.")
            sys.exit()

    hoje = obter_data_atual()

    while True:
        print(
            f'Deseja baixar os dados, ou ler arquivo já existente? Digite "baixar" ou "ler". Se quiser sair digite "cancelar".\n'
        )
        ans = input("Vamos: ")
        if ans == "baixar":
            pedidos_hostgator = consultar_pedidos(DLOJAVIRTUAL_URL, DLOJAVIRTUAL_KEY)
            break
        if ans == "ler":
            try:
                pedidos_hostgator = pd.read_csv("Hostgator - Últimos 5000 pedidos.csv")
                break
            except:
                print("Não foi possível ler o arquivo e iremos baixá-lo.")
                pedidos_hostgator = pd.read_csv("Hostgator - Últimos 5000 pedidos.csv")
                break
        if ans == "cancelar":
            sys.exit()

    data_inicial = input(
        f"Qual a data inicial para o período de apuração das NFes? Digite no formato AAAA-MM-DD."
    )
    data_final = input(
        f"Qual a data final para o período de apuração das NFes? Digite no formato AAAA-MM-DD."
    )

    data_inicial = str(f"{data_inicial} 00:00:00")
    data_final = str(f"{data_final} 00:00:00")

    nfes_bling = obter_pedidos_nfes_todas_paginas(
        limite_por_pagina=100,
        access_token=ACCESS_TOKEN,
        tipo=1,
        dataEmissaoInicial=data_inicial,
        dataEmissaoFinal=data_final,
    )

    print(nfes_bling)

    nfes_bling["bling_data_de_emissao"] = pd.to_datetime(
        nfes_bling["bling_data_de_emissao"]  # , infer_datetime_format=True
    )
    nfes_bling["mes_de_emissao_de_nf"] = nfes_bling["bling_data_de_emissao"].dt.month
    nfes_bling["ano_de_emissao_de_nf"] = nfes_bling["bling_data_de_emissao"].dt.year

    detalhes_nfes = nfes_bling.copy()
    # detalhes_nfes = detalhes_nfes[:10]

    tqdm.pandas()

    detalhes_nfes["volumes"] = detalhes_nfes.progress_apply(
        lambda row: obter_detalhe_nfe_por_pedido(
            id_nota_fiscal=row["bling_id"], access_token=ACCESS_TOKEN
        ),
        axis=1,
    )

    detalhes_nfes = expandir_dicionario_para_colunas_tratado(detalhes_nfes, "volumes")

    detalhes_nfes = expandir_dicionario_para_colunas_tratado(
        detalhes_nfes, "volumes_bling_volumes"
    )

    detalhes_nfes = expandir_dicionario_para_colunas_tratado(
        detalhes_nfes, "volumes_bling_transporte"
    )

    detalhes_nfes = expandir_dicionario_para_colunas_tratado(
        detalhes_nfes, "volumes_bling_transporte_transportador"
    )

    total_pedidos = len(
        detalhes_nfes[
            detalhes_nfes["volumes_bling_transporte_transportador_nome"]
            == "TEX COURIER S.A"
        ]
    )

    print(f"São {total_pedidos} para processar.")

    df_de_pedidos_ttex = detalhes_nfes[
        detalhes_nfes["volumes_bling_transporte_transportador_nome"]
        == "TEX COURIER S.A"
    ]

    lista_de_pedidos_ttex = df_de_pedidos_ttex["volumes_bling_lista_de_volumes"].sum()

    df_detalhes_objetos_nfes = obter_detalhes_objetos_logistica(
        lista_de_pedidos_ttex, access_token=access_token
    )

    df_detalhes_objetos_nfes = pd.DataFrame(df_detalhes_objetos_nfes)

    df_detalhes_objetos_nfes = expandir_dicionario_para_colunas(
        df_detalhes_objetos_nfes, "dimensao"
    )

    df_detalhes_objetos_nfes = expandir_dicionario_para_colunas(
        df_detalhes_objetos_nfes, "notaFiscal"
    )

    df_detalhes_objetos_nfes = df_detalhes_objetos_nfes.set_index("notaFiscal_id")

    df_de_pedidos_ttex["bling_id"] = df_de_pedidos_ttex["bling_id"].astype("Int64")

    df_de_pedidos_ttex = df_de_pedidos_ttex.set_index("bling_id")

    df_notas_fiscais_com_detalhes = df_detalhes_objetos_nfes.join(
        df_de_pedidos_ttex, lsuffix="details_", rsuffix="nfes_"
    )

    df_notas_fiscais_com_detalhes["bling_numero"] = df_notas_fiscais_com_detalhes[
        "bling_numero"
    ].astype("Int64")

    df_notas_fiscais_com_detalhes = df_notas_fiscais_com_detalhes.set_index(
        "bling_numero"
    )

    ans = input(
        f'Deseja salvar as informações de nota fiscal agora? Digite "s" para salvar. Sua resposta é: '
    )
    if ans == "s":
        df_notas_fiscais_com_detalhes.to_csv(
            f"Dados Bling NFes - de {data_inicial} a {data_final}.csv"
        )

    df_notas_fiscais_com_detalhes["bling_endereco_cep_tratado"] = (
        df_notas_fiscais_com_detalhes["bling_endereco_cep"].str.replace(
            r"[.\-]", "", regex=True
        )
    )

    cobranca = str(input("Digite o nome do arquivo de cobranca"))

    dados = pd.read_csv(f"./data/{cobranca}.csv", sep=";", encoding="unicode_escape")

    dados = dados[
        [
            "CTe",
            "AWB",
            "Nota Fiscal",
            "Cidade",
            "CEP",
            "UF",
            "ROTA",
            "Data Encomenda",
            "Data Faturamento",
            "Peso",
            "Valor NF",
            "Tipo Servico",
            "Tipo do Frete",
            "VL COD",
            "Valor Postagem",
            "Seguro",
            "Gris",
            "Frete",
            "Desconto Frete",
            "Outros",
            "ICMS",
            "%ICMS",
            "Frete Valor c/ ICMS",
            " Frete c/ ICMS",
            "%Outros C/ ICMS",
            "Total Servico",
            "Download Xml CTe",
        ]
    ]

    dados = dados[~dados["Nota Fiscal"].isna()]

    dados["Nota Fiscal"] = dados["Nota Fiscal"].astype(int).copy()
    dados = dados.set_index("Nota Fiscal")

    lista_de_pedidos = "Hostgator - Últimos 5000 pedidos.csv"

    pedidos_de_venda = pd.read_csv(f"{lista_de_pedidos}.csv")
    pedidos_de_venda = pedidos_de_venda.drop("Unnamed: 0", axis=1)
    pedidos_de_venda["Item"] = pedidos_de_venda["Item"].apply(converter_lista)

    pedidos_de_venda_nao_nulos = pedidos_de_venda[
        pedidos_de_venda["Wspedido.nota_fiscal"].notna()
    ]
    pedidos_de_venda_nulos = pedidos_de_venda[
        ~pedidos_de_venda["Wspedido.nota_fiscal"].notna()
    ]

    print(
        f"São {len(pedidos_de_venda_nao_nulos)} pedidos não nulos e {len(pedidos_de_venda_nulos)} nulos."
    )

    pedidos_de_venda_nao_nulos["Wspedido.nota_fiscal"] = (
        pedidos_de_venda_nao_nulos["Wspedido.nota_fiscal"].astype(int).copy()
    )
    pedidos_de_venda_nao_nulos = pedidos_de_venda_nao_nulos.set_index(
        "Wspedido.nota_fiscal"
    )

    teste = dados.join(
        pedidos_de_venda_nao_nulos, how="left", lsuffix="ttex_", rsuffix="painel_"
    )

    teste = teste.join(
        df_notas_fiscais_com_detalhes, lsuffix="ttex_painel_", rsuffix="bling_"
    )

    # Aplicar a correção nas colunas 'Peso' e 'Wspedido.total_peso'
    cols_to_fix_with_numbers_greater_than_one = ["Wspedido.total_peso"]
    for col in cols_to_fix_with_numbers_greater_than_one:
        teste[col] = teste[col].apply(corrigir_separadores)

    # Substituir separadores de milhar e decimal nas colunas 'Peso' e 'Wspedido.total_peso'
    cols_to_fix = ["Peso", "Total Servico", "Valor NF"]

    for col in cols_to_fix:
        teste[col] = (
            teste[col]
            .astype(str)  # Garantir que os valores sejam strings para a substituição
            .str.replace(
                ".", "", regex=False
            )  # Remover pontos como separadores de milhar
            .str.replace(
                ",", ".", regex=False
            )  # Substituir vírgulas pelo ponto decimal
        )

    cols_to_fix_just_the_separator = [
        "Frete",
    ]

    for col in cols_to_fix_just_the_separator:
        teste[col] = teste[col].astype(str).str.replace(",", ".", regex=False)

    # Converter as colunas para float
    teste[cols_to_fix] = teste[cols_to_fix].apply(pd.to_numeric, errors="coerce")

    # Garantir que a coluna 'CEP' permaneça como string
    teste["CEP"] = teste["CEP"].astype(str)

    # Aplicar os métodos apenas em valores válidos
    teste["peso_caixa"] = teste["Wspedido.total_peso"].apply(
        lambda x: adicionar_peso_caixa(x) if pd.notna(x) else np.nan
    )
    teste["peso_cubado"] = teste["Wspedido.total_peso"].apply(
        lambda x: calcular_peso_cubado(x) if pd.notna(x) else np.nan
    )

    # Calcular 'peso_calculado'
    teste["peso_calculado"] = teste["Wspedido.total_peso"] + teste["peso_caixa"]

    # Calcular a diferença absoluta
    teste["diff_pesos_absoluta"] = teste["Peso"] - teste["peso_calculado"]

    # Calcular a diferença percentual com tratamento para divisão por zero
    teste["diff_pesos_percentual"] = np.where(
        teste["peso_calculado"] != 0,
        teste["Peso"] / teste["peso_calculado"] - 1,
        np.nan,
    )

    # Cálculo da diferença de peso absoluta e relativa
    teste["diff_pesos_absoluta"] = teste["Peso"] - teste["peso_calculado"]
    teste["diff_pesos_percentual"] = teste["Peso"] / teste["peso_calculado"]

    # Implementação da validação entre peso real e peso cubico
    teste["escolha_metodo_pesagem"] = (
        teste[["peso_calculado", "peso_cubado"]]
        .copy()
        .apply(escolha_metodo_pesagem, axis=1)
    )

    # Sorteando
    teste = teste.sort_values(["diff_pesos_percentual"], ascending=False)
    teste["categoria_peso"] = teste["peso_calculado"].apply(intervalo_peso)
    teste["categoria_peso"] = teste["categoria_peso"].astype("Int64")

    file = f"./Tabela de Preços.xlsx"

    tabela_de_precos = pd.read_excel(
        file, sheet_name="Tabela_Unificada", usecols="B:EJ", header=8, nrows=35
    )

    file = f"./abrangência.xlsx"

    abrangencia = pd.read_excel(
        file,
        usecols="B:L",
        header=5,
        dtype={"CEP Inicial": object, "CEP Final": object},
    )
    abrangencia["CEP Inicial"] = abrangencia["CEP Inicial"].astype("Float64")
    abrangencia["CEP Inicial"] = abrangencia["CEP Inicial"].astype("Int64")
    abrangencia["CEP Final"] = abrangencia["CEP Final"].astype("Float64")
    abrangencia["CEP Final"] = abrangencia["CEP Final"].astype("Int64")

    teste["bling_endereco_cep"] = teste["bling_endereco_cep"].astype("str")

    teste["CEP_3"] = teste["bling_endereco_cep"].apply(get_cep_number)
    teste["CEP_3"] = teste["CEP_3"].astype("Float64")
    teste["CEP_3"] = teste["CEP_3"].astype("Int64")

    teste["CEP"] = teste["CEP"].astype("Float64")
    teste["CEP"] = teste["CEP"].astype("Int64")

    df_b = abrangencia.copy()
    df_b = df_b.dropna()
    df_a = teste
    # Preenchendo valores nulos com um valor específico
    fill_value = pd.NA
    df_b.fillna(fill_value, inplace=True)

    # Transformando o índice em uma tupla de intervalo de CEP
    df_b.index = pd.IntervalIndex.from_tuples(
        list(zip(df_b["CEP Inicial"], df_b["CEP Final"])), closed="both"
    )

    # Aplicando a função em cada valor da coluna 'CEP' do DataFrame 'A'
    df_a["Geografia Comercial"] = df_a["CEP_3"].apply(
        lambda x: encontrar_geografia_comercial(x, df_b)
    )

    # Especificando as colunas de interesse na origem
    colunas_interesse = ["Geografia Comercial", "Risco", "Prazo"]

    # Especificando o Nome das colunas no destino
    novos_nomes_colunas = [
        "Geografia_Comercial_tbAbran",
        "Risco_tbAbran",
        "Prazo_tbAbran",
    ]

    df_a[novos_nomes_colunas] = (
        df_a["CEP_3"]
        .apply(
            lambda x: pd.Series(
                encontrar_informacoes_tabela_abrangencia(x, df_b, colunas_interesse)
            )
        )
        .rename(columns=dict(zip(colunas_interesse, novos_nomes_colunas)))
    )

    # Aplicando a função em cada linha do DataFrame 'A'
    df_a["frete_peso_calculado"] = df_a.apply(
        lambda row: consultar_valor_geografia_comercial_metodo_pesagem(
            row, tabela_de_precos
        ),
        axis=1,
    )

    df_a.to_csv(f"Análise de Pagamento - {cobranca}.csv")
