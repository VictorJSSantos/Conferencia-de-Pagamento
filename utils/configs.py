import base64
from dotenv import load_dotenv, find_dotenv, set_key
import os
from urllib.parse import urlencode

from utils.request import *

env_path = ".env"
env_file = find_dotenv(env_path)
load_dotenv(env_file)

DLOJAVIRTUAL_KEY = os.environ.get("DLOJAVIRTUAL_KEY")
BLING_CLIENT_ID = os.environ.get("BLING_CLIENT_ID")
BLING_CLIENT_SECRET = os.environ.get("BLING_CLIENT_SECRET")
BLING_REDIRECT_URI = os.environ.get("BLING_REDIRECT_URI")
STATE = os.environ.get("STATE")

# Chaves esperadas no JSON de autorização
TOKEN_KEYS = ["ACCESS_TOKEN", "EXPIRES_IN", "TOKEN_TYPE", "SCOPE", "REFRESH_TOKEN"]


# Função para salvar o JSON de tokens no .env
def save_tokens(tokens_dict):
    """
    Salva o json de tokens no arquivo .env.
    """
    for key, value in tokens_dict.items():
        if key in tokens_dict:  # Garantir que só as chaves esperadas sejam salvas
            set_key(env_path, key, str(value))  # Salva como UPPERCASE no .env
    print("Tokens salvos no .env com sucesso.")


# Função para carregar os tokens do .env como um dicionário
def load_tokens(tokens_list):
    """
    Carrega os tokens do arquivo .env como um dicionário.
    """
    load_dotenv()
    tokens = {}
    for key in tokens_list:
        value = os.getenv(key)  # As chaves foram salvas em UPPERCASE
        if value is not None:
            tokens[key] = value
    return tokens


# Função para obter o `access_token` com validação
def get_access_token():
    """
    Obtém o token de acesso diretamente do .env.
    """
    load_dotenv()
    access_token = os.getenv("access_token")
    print(f"O get_acess_token deu o token {access_token}")
    if access_token:
        return access_token
    else:
        print("Access token não encontrado. É necessário autenticar.")
        return None


def refresh_or_get_access_token():
    """
    Função para obter o token de acesso, renová-lo se necessário ou autenticar novamente.
    """
    while True:
        try:
            # Verificar se já temos um access token válido
            ACCESS_TOKEN = get_access_token()
            if ACCESS_TOKEN:
                # Verificar a validade do token
                headers = {
                    "Authorization": f"Bearer {ACCESS_TOKEN}",
                    "Accept": "application/json",
                }
                url_validator = "https://www.bling.com.br/Api/v3/logisticas"
                response = requests.get(url_validator, headers=headers)

                if response.status_code == 200:
                    print("Token válido encontrado. Prosseguindo!")
                    return ACCESS_TOKEN
                else:
                    print("Token expirado ou inválido. Tentando renovar...")

            # Obter dados do .env para renovação ou nova autenticação
            BLING_CLIENT_ID = os.getenv("BLING_CLIENT_ID")
            BLING_CLIENT_SECRET = os.getenv("BLING_CLIENT_SECRET")
            BLING_REDIRECT_URI = os.getenv("BLING_REDIRECT_URI")
            REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

            if REFRESH_TOKEN:
                # Tentar renovar o token com o refresh token
                token_url = "https://www.bling.com.br/Api/v3/oauth/token"
                token_data = {
                    "grant_type": "refresh_token",
                    "refresh_token": REFRESH_TOKEN,
                    "client_id": BLING_CLIENT_ID,
                    "client_secret": BLING_CLIENT_SECRET,
                }
                auth_headers = {
                    "Authorization": f'Basic {base64.b64encode(f"{BLING_CLIENT_ID}:{BLING_CLIENT_SECRET}".encode()).decode()}',
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                }

                response = requests.post(
                    token_url, data=token_data, headers=auth_headers
                )

                if response.status_code == 200:
                    # Atualizar tokens no .env
                    tokens = response.json()
                    save_tokens(tokens)
                    print("Token renovado com sucesso!")
                    return tokens.get("access_token")
                else:
                    print(
                        "Falha ao renovar o token. Código de status:",
                        response.status_code,
                    )

            # Caso não seja possível renovar, iniciar nova autenticação
            print("Iniciando nova autenticação.")
            invite_url = input("Insira a URL de convite: ")
            state = invite_url[-32:]

            authorization_url = "https://www.bling.com.br/Api/v3/oauth/authorize"
            authorization_params = {
                "response_type": "code",
                "client_id": BLING_CLIENT_ID,
                "redirect_uri": BLING_REDIRECT_URI,
                "state": state,
            }

            authorization_url_with_params = (
                f"{authorization_url}?{urlencode(authorization_params)}"
            )
            print(
                f"Por favor, visite a seguinte URL e autorize o aplicativo:\n{authorization_url_with_params}"
            )

            # Após autorização, solicitar o código de autorização
            authorization_code = input("Insira o código de autorização: ")

            # Solicitar novos tokens
            token_data = {
                "grant_type": "authorization_code",
                "code": authorization_code,
                "client_id": BLING_CLIENT_ID,
                "client_secret": BLING_CLIENT_SECRET,
                "redirect_uri": BLING_REDIRECT_URI,
            }

            response = requests.post(token_url, data=token_data, headers=auth_headers)

            if response.status_code == 200:
                # Salvar novos tokens no .env
                tokens = response.json()
                save_tokens(tokens)
                print("Nova autenticação realizada com sucesso!")
                return tokens.get("access_token")
            else:
                print("Erro na autenticação. Código de status:", response.status_code)
                print(response.text)

        except Exception as e:
            print(f"Ocorreu um erro: {e}")
            quit_option = input('Digite "cancelar" para sair do programa: ')
            if quit_option.lower() == "cancelar":
                break
