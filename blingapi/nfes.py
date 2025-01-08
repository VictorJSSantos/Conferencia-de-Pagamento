import pandas as pd


from utils.configs import *
from utils.request import *


def obter_detalhe_nfe_por_pedido(id_nota_fiscal=None, **kwargs):
    api_url = f"https://www.bling.com.br/Api/v3/nfe/{id_nota_fiscal}"

    if not kwargs.get("access_token"):
        kwargs["access_token"] = get_access_token()
        return kwargs[
            "access_token"
        ]  # Retorna None se o token de acesso não estiver disponível

    if not id_nota_fiscal:
        print(f"Não foi declarada a NFe para ter as informações extraídas")
        return None

    headers = {
        "Authorization": f'Bearer {kwargs["ACCESS_TOKEN"]}',
        "Accept": "application/json",
    }

    # Use a função decorada para fazer a requisição com controle de taxa
    try:
        response = fazer_requisicao(api_url, headers=headers)
        data = response.json()["data"]
        print(data)
        print(f"{type(data)}, {type(response), {response.status_code}}")

        if not data:
            print("Sem dados disponíveis. if not data")
            return None

        resultados_pagina = []

        # for pedido_data in data:
        bling_id = data.get("id", None)
        bling_tipo = data.get("tipo", None)
        bling_situacao = data.get("situacao", None)

        bling_transporte = data.get("transporte", None)
        bling_transportador = bling_transporte.get("nome", None)

        bling_etiqueta = bling_transporte.get("etiqueta", None)
        bling_cep_etiqueta = bling_etiqueta.get("cep", None)
        bling_uf_etiqueta = bling_etiqueta.get("uf", None)
        bling_cidade_etiqueta = bling_etiqueta.get("municipio", None)

        bling_volumes = bling_transporte.get("volumes", None)
        ids_volumes = [volume["id"] for volume in bling_volumes]
        bling_lista_de_volumes = ids_volumes

        bling_valor_nota_fiscal = data.get("valorNota", None)
        bling_chave_acesso = data.get("chaveAcesso", None)
        bling_link_danfe = data.get("linkDanfe", None)
        bling_numero_pedido_loja = data.get("numeroPedidoLoja", None)

        resultado = {
            "bling_id": bling_id,
            "bling_tipo": bling_tipo,
            "bling_situacao": bling_situacao,
            "bling_transporte": bling_transporte,
            "bling_transportador": bling_transportador,
            "bling_etiqueta": bling_etiqueta,
            "bling_cep_etiqueta": bling_cep_etiqueta,
            "bling_uf_etiqueta": bling_uf_etiqueta,
            "bling_cidade_etiqueta": bling_cidade_etiqueta,
            "bling_valor_nota_fiscal": bling_valor_nota_fiscal,
            "bling_chave_acesso": bling_chave_acesso,
            "bling_link_danfe": bling_link_danfe,
            "bling_numero_pedido_loja": bling_numero_pedido_loja,
            "bling_volumes": bling_volumes,
            "bling_lista_de_volumes": bling_lista_de_volumes,
        }

        return resultado

    except Exception as e:
        print(f"Erro na requisição: {str(e)}")
        return None


def obter_pedidos_nfes_todas_paginas(limite_por_pagina=100, **kwargs):
    api_url = "https://www.bling.com.br/Api/v3/nfe"

    if not kwargs.get("access_token"):
        kwargs["access_token"] = get_access_token()
        return kwargs[
            "access_token"
        ]  # Retorna None se o token de acesso não estiver disponível

    headers = {
        "Authorization": f'Bearer {kwargs["access_token"]}',
        "Accept": "application/json",
    }

    resultados_totais = []

    try:
        pagina = 1

        while True:
            params = {
                "pagina": pagina,
                "limite": limite_por_pagina,
                "numeroLoja": kwargs.get("numeroLoja"),
                "situacao": kwargs.get("idContato"),
                "dataEmissaoInicial": kwargs.get("dataEmissaoInicial"),
                "dataEmissaoFinal": kwargs.get("dataEmissaoFinal"),
            }

            # Use a função decorada para fazer a requisição com controle de taxa
            response = fazer_requisicao(api_url, headers=headers, params=params)

            if response.status_code == 200:
                data = response.json()["data"]

                if not data:
                    print("Sem dados disponíveis. - todas as paginas if not data")
                    return None

                resultados_pagina = []

                for pedido_data in data:
                    bling_id = pedido_data.get("id", None)
                    bling_tipo = pedido_data.get("tipo", None)
                    bling_situacao = pedido_data.get("situacao", None)
                    bling_numero = pedido_data.get("numero", None)
                    bling_data_de_emissao = pedido_data.get("dataEmissao", None)
                    bling_data_de_operacao = pedido_data.get("dataOperacao", None)
                    bling_contato = pedido_data.get("contato", None)

                    bling_contato_id = bling_contato.get("id", None)
                    bling_contato_nome = bling_contato.get("nome", None)
                    bling_tipo_pessoa_contato = bling_contato.get("tipoPessoa", None)

                    bling_endereco = bling_contato.get("endereco", None)
                    bling_endereco_cep = bling_endereco.get("cep", None)
                    bling_endereco_uf = bling_endereco.get("uf", None)
                    bling_endereco_cidade = bling_endereco.get("municipio", None)

                    resultado = {
                        "bling_id": bling_id,
                        "bling_tipo": bling_tipo,
                        "bling_situacao": bling_situacao,
                        "bling_numero": bling_numero,
                        "bling_data_de_emissao": bling_data_de_emissao,
                        "bling_data_de_operacao": bling_data_de_operacao,
                        "bling_contato": bling_contato,
                        "bling_contato_id": bling_contato_id,
                        "bling_contato_nome": bling_contato_nome,
                        "bling_tipo_pessoa_contato": bling_tipo_pessoa_contato,
                        "bling_endereco": bling_endereco,
                        "bling_endereco_cep": bling_endereco_cep,
                        "bling_endereco_uf": bling_endereco_uf,
                        "bling_endereco_cidade": bling_endereco_cidade,
                    }

                    resultados_pagina.append(resultado)

                # Adiciona os resultados da página atual à lista total
                resultados_totais.append(pd.DataFrame(resultados_pagina))

                # Verifica se há mais páginas para obter
                if len(resultados_pagina) < limite_por_pagina:
                    break

                # Avança para a próxima página
                pagina += 1

            else:
                print(
                    f"Erro na obtenção dos pedidos de vendas da pagina {pagina}. Código de status: {response.status_code}"
                )
                # rint(response.text)
                # return None

        # Concatena todos os DataFrames em um único DataFrame
        headers = list(resultado.keys())
        df_resultado = pd.concat(resultados_totais, ignore_index=True)
        return df_resultado

    except Exception as e:
        print(f"Erro na requisição: {str(e)}")
        return None


def obter_detalhe_nfe_por_pedido(id_nota_fiscal=None, **kwargs):
    api_url = f"https://www.bling.com.br/Api/v3/nfe/{id_nota_fiscal}"

    if not kwargs.get("access_token"):
        kwargs["access_token"] = get_access_token()
        return kwargs[
            "access_token"
        ]  # Retorna None se o token de acesso não estiver disponível

    if not id_nota_fiscal:
        print(f"Não foi declarada a NFe para ter as informações extraídas")
        return None

    headers = {
        "Authorization": f'Bearer {kwargs["access_token"]}',
        "Accept": "application/json",
    }

    # Use a função decorada para fazer a requisição com controle de taxa
    try:
        response = fazer_requisicao(api_url, headers=headers)
        data = response.json()["data"]

        if not data:
            print("Sem dados disponíveis. - nfe por pedido if not data")
            return None

        resultados_pagina = []

        # for pedido_data in data:
        bling_id = data.get("id", None)
        bling_tipo = data.get("tipo", None)
        bling_situacao = data.get("situacao", None)

        bling_transporte = data.get("transporte", None)
        bling_transportador = bling_transporte.get("nome", None)

        bling_etiqueta = bling_transporte.get("etiqueta", None)
        bling_cep_etiqueta = bling_etiqueta.get("cep", None)
        bling_uf_etiqueta = bling_etiqueta.get("uf", None)
        bling_cidade_etiqueta = bling_etiqueta.get("municipio", None)

        bling_volumes = bling_transporte.get("volumes", None)
        ids_volumes = [volume["id"] for volume in bling_volumes]
        bling_lista_de_volumes = ids_volumes

        bling_valor_nota_fiscal = data.get("valorNota", None)
        bling_chave_acesso = data.get("chaveAcesso", None)
        bling_link_danfe = data.get("linkDanfe", None)
        bling_numero_pedido_loja = data.get("numeroPedidoLoja", None)

        resultado = {
            "bling_id": bling_id,
            "bling_tipo": bling_tipo,
            "bling_situacao": bling_situacao,
            "bling_transporte": bling_transporte,
            "bling_transportador": bling_transportador,
            "bling_etiqueta": bling_etiqueta,
            "bling_cep_etiqueta": bling_cep_etiqueta,
            "bling_uf_etiqueta": bling_uf_etiqueta,
            "bling_cidade_etiqueta": bling_cidade_etiqueta,
            "bling_valor_nota_fiscal": bling_valor_nota_fiscal,
            "bling_chave_acesso": bling_chave_acesso,
            "bling_link_danfe": bling_link_danfe,
            "bling_numero_pedido_loja": bling_numero_pedido_loja,
            "bling_volumes": bling_volumes,
            "bling_lista_de_volumes": bling_lista_de_volumes,
        }

        return resultado

    except Exception as e:
        print(f"Erro na requisição: {str(e)}")
        return None
