import json
import requests
import sys
from tqdm import tqdm

from blingapi.nfes import *

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
