import pandas as pd


from utils.configs import *
from utils.request import *


# Chaves esperadas no JSON de autorização
TOKENS_HOSTGATOR = ["DLOJAVIRTUAL_KEY", "DLOJAVIRTUAL_URL"]

tokens = load_tokens(TOKENS_HOSTGATOR)

DLOJAVIRTUAL_KEY = tokens["DLOJAVIRTUAL_KEY"]
DLOJAVIRTUAL_URL = tokens["DLOJAVIRTUAL_URL"]


# Configurar os cabeçalhos da requisição
headers = {"Content-Type": "application/json", "appKey": DLOJAVIRTUAL_KEY}


def consultar_pedidos(
    url_loja=DLOJAVIRTUAL_URL, api_key=DLOJAVIRTUAL_KEY, limit=50, count=5000
):
    # Endpoint base da API
    endpoint = f"https://{url_loja}/ws/wspedidos.json"

    # Lista para armazenar os resultados
    all_results = []

    # Contagem de registros
    iteration_count = 0

    # Iniciar com a página 1
    current_page = 1

    while True:
        # Parâmetros para a paginação
        params = {"page": current_page, "limit": limit, "count": count}

        try:
            # Fazer a solicitação GET à API
            response = requests.get(endpoint, headers=headers, params=params)

            # Verificar se a solicitação foi bem-sucedida (código de status 2XX)
            if response.status_code // 100 == 2:
                # Converter a resposta JSON para um dicionário Python
                json_response = response.json()

                # Verificar se a resposta contém a chave 'result'
                if "result" in json_response:
                    # Adicionar os resultados à lista
                    all_results.extend(json_response["result"])

                    # Verificar se há mais páginas
                    if json_response["pagination"]["has_next_page"]:
                        # Atualizar a página atual
                        if current_page % 50 == 0:
                            print("Processando página: ", current_page)

                        iteration_count += 50

                        if iteration_count >= count:
                            print(f"Atingido o limite de registros de {count}")
                            break

                        current_page += 1

                    else:
                        # Se não houver mais páginas, sair do loop
                        break
                else:
                    print(f"Resposta inesperada da API: {json_response}")
                    break
            else:
                print(f"Erro na solicitação: {response.status_code}")
                print(response.text)
                break

        except Exception as e:
            print(f"Erro na solicitação: {e}")
            break

    # Criar um DataFrame pandas a partir da lista de resultados
    df = pd.json_normalize(all_results)

    ans = input(
        f"Quer salvar dados do site sobre os últimos {count} pedidos? Se SIM, digite 's', qualquer outra coisa não salvará."
    )
    if ans == "s":
        # Salvar o DataFrame combinado de volta ao arquivo CSV
        df.to_csv(f"./Hostgator - Últimos {count} pedidos.csv", index=False)

    return df
