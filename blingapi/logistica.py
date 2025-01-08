from utils.configs import *
from utils.format import *
from utils.request import *
from utils.weight import *


def obter_detalhes_objeto_logistica(id_objeto, access_token=None):
    api_url = f"https://www.bling.com.br/Api/v3/logisticas/objetos/{id_objeto}"

    if not access_token:
        access_token = get_access_token()  # Função para obter token de acesso

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    try:
        response = fazer_requisicao(api_url, headers=headers)
        if response.status_code == 200:
            return response.json()["data"]
        else:
            print(
                f"Erro ao obter detalhes do objeto logístico. Código de status: {response.status_code}"
            )
            return None
    except Exception as e:
        print(f"Erro na requisição: {str(e)}")
        return None


def obter_detalhes_objetos_logistica(lista_ids_objetos, access_token=None):
    detalhes_objetos = []
    for id_objeto in lista_ids_objetos:
        detalhes_objeto = obter_detalhes_objeto_logistica(id_objeto, access_token)
        if detalhes_objeto:
            detalhes_objetos.append(detalhes_objeto)
    return detalhes_objetos
