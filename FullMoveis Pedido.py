import mysql.connector
import requests
import subprocess

subprocess.run(['python', 'FullMoveis NF.py'])
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
    url_ped = f'https://bling.com.br/Api/v2/pedidos/page={page}/json/?&apikey={apikey}'
    parametro_ped = {
        'filters': f'dataEmissao[{data_ini} TO {data_fin}]',
        'apikey': apikey
    }
    requisicao_ped = requests.get(url_ped, params=parametro_ped)
    if requisicao_ped.status_code != 200:
        print(f'Erro ao obter pedidos, código de status: {requisicao_ped.status_code}')
        break
    informacoes_ped = requisicao_ped.json()

    if 'pedidos' in informacoes_ped.get('retorno', {}):
        pedidos = informacoes_ped['retorno']['pedidos']
        num_pedidos = len(pedidos)
        print(f'Recebidos {num_pedidos} pedidos na página {page}')

        if num_pedidos == 0:
            print("Nenhum pedido recebido. Saindo do loop.")
            break
        
        for pedido in pedidos:
            info_ped = pedido.get('pedido', {})
            dados_ped = {
                'Número_Pedido': info_ped.get('numero', ''),
                'Emissão_Pedido': info_ped.get('data', ''),
                'Pedido_MKT': info_ped.get('numeroPedidoLoja', ''),
                'Situação_Pedido': info_ped.get('situacao', ''),
                'Marketplace': info_ped.get('loja', ''),
                'Valor_Frete': info_ped.get('valorfrete', ''),
                'Valor_Total_Produtos': info_ped.get('totalprodutos', ''),
                'Valor_Total_Venda': info_ped.get('totalvenda', ''),
                'UF': info_ped.get('cliente', {}).get('uf', ''),
                'Cidade': info_ped.get('cliente', {}).get('cidade', '')
            }
            
            # Verificar se o número do pedido já existe na tabela
            cursor.execute("SELECT * FROM pedidos WHERE n_pd = %s", (dados_ped['Número_Pedido'],))
            existing_row = cursor.fetchone()

            if existing_row:
                # Atualizar os dados correspondentes se o número do pedido já existe
                update_query = """
                UPDATE pedidos
                SET emissao_pd = %(Emissão_Pedido)s, pd_marketplace = %(Pedido_MKT)s, status_pd = %(Situação_Pedido)s,
                    mkt = %(Marketplace)s, Valor_Frete = %(Valor_Frete)s, valor_total_produtos = %(Valor_Total_Produtos)s,
                    valor_total_venda = %(Valor_Total_Venda)s, uf = %(UF)s, cidade = %(Cidade)s
                WHERE n_pd = %(Número_Pedido)s
                """
                cursor.execute(update_query, dados_ped)
                conexao_mysql.commit()
            else:
                # Inserir um novo registro se o número do pedido não existe
                insert_query = """
                INSERT INTO pedidos (n_pd, emissao_pd, pd_marketplace, status_pd, mkt, valor_frete,
                                            valor_total_produtos, valor_total_venda, uf, cidade)
                VALUES (%(Número_Pedido)s, %(Emissão_Pedido)s, %(Pedido_MKT)s, %(Situação_Pedido)s, %(Marketplace)s, %(Valor_Frete)s,
                        %(Valor_Total_Produtos)s, %(Valor_Total_Venda)s, %(UF)s, %(Cidade)s)
                """
                cursor.execute(insert_query, dados_ped)
                conexao_mysql.commit()

    else:
        print("Nenhum pedido encontrado na página", page)
        break

    page += 1

# Fechar cursor e conexão
cursor.close()
conexao_mysql.close()

print("Dados importados e atualizados com sucesso no banco de dados!")
