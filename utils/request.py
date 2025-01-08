import requests
from ratelimit import limits, sleep_and_retry


# Decorator para limitar o número de chamadas por segundo
@sleep_and_retry
@limits(calls=1, period=1)
def fazer_requisicao(api_url, headers, params=None):
    # Coloque aqui sua lógica de requisição
    response = requests.get(api_url, headers=headers, params=params)
    return response
