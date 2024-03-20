import mysql.connector
import requests

# Conectar ao banco de dados MySQL
conexao_mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="200623",
    database="fullmoveis"
)

# Verificar se a conexão foi bem sucedida
if conexao_mysql.is_connected():
    print("Conexão bem sucedida")

# Criar um cursor
cursor = conexao_mysql.cursor()

apikey = 'd3731b0efe80dcfc633e0e1aa2f6fd12fe981774d5665bdcbdc0ab58033a316c7b0b9a77'
page = 1
data_ini = '01/01/2024'
data_fin = '31/12/2024'

while True:
    url_nf = f'https://bling.com.br/Api/v2/notasfiscais/page={page}/json/?&apikey={apikey}'
    parametro_nf = {
        'filters': f'dataEmissao[{data_ini} TO {data_fin}]',
        'apikey': apikey
    }
    requisicao_nf = requests.get(url_nf, params=parametro_nf)
    if requisicao_nf.status_code != 200:
        print(f'Erro ao obter notas fiscais, código de status: {requisicao_nf.status_code}')
        break
    informacoes_nf = requisicao_nf.json()

    if 'notasfiscais' in informacoes_nf.get('retorno', {}):
        notasfiscais = informacoes_nf['retorno']['notasfiscais']
        num_nf = len(notasfiscais)
        print(f'Recebidos {num_nf} pedidos na página {page}')

        if num_nf == 0:
            print("Nenhum pedido recebido. Saindo do loop.")
            break

        for notafiscal in notasfiscais:
            info_nf = notafiscal.get('notafiscal', {})
            dados_nf = {
                'NF': info_nf.get('numero', ''),
                'Emissão_NF': info_nf.get('dataEmissao', ''),
                'Pedido_MKT': info_nf.get('numeroPedidoLoja', ''),
                'Valor_NF': info_nf.get('valorNota', ''),
                'Situação_NF': info_nf.get('situacao', '')
            }
            info_transp = info_nf.get('transporte', {})
            dados_transp = {
                'Transportadora_NF': info_transp.get('transportadora', '')
            }

            dados_completo_nf = {**dados_nf, **dados_transp}
            
            # Verificar se o número do pedido já existe na tabela
            cursor.execute("SELECT * FROM nfs WHERE nf = %s", (dados_completo_nf['NF'],))
            existing_row = cursor.fetchone()

            if existing_row:
                # Atualizar os dados correspondentes se o número do pedido já existe
                update_query = """
                UPDATE nfs
                SET nf = %(NF)s, emissao_nf = %(Emissão_NF)s, pd_mkt = %(Pedido_MKT)s,
                    valor_nf = %(Valor_NF)s, status_nf = %(Situação_NF)s, transp = %(Transportadora_NF)s
                WHERE nf = %(NF)s
                """
                cursor.execute(update_query, dados_completo_nf)
                conexao_mysql.commit()
            else:
                # Inserir um novo registro se o número do pedido não existe
                insert_query = """
                INSERT INTO nfs (nf, emissao_nf, pd_mkt, valor_nf, status_nf, transp)
                VALUES (%(NF)s, %(Emissão_NF)s, %(Pedido_MKT)s, %(Valor_NF)s, %(Situação_NF)s, %(Transportadora_NF)s)
                """
                cursor.execute(insert_query, dados_completo_nf)
                conexao_mysql.commit()

    else:
        print("Nenhum pedido encontrado na página", page)
        break

    page += 1

# Fechar cursor e conexão
cursor.close()
conexao_mysql.close()

print("Dados importados e atualizados com sucesso no banco de dados!")
