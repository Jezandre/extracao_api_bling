 Extração de dados do Bling

## Introdução

A plataforma bling é um sistema de gestão online para diversos tipos de negócio. Eles oferecem uma Api que permite a integração com diversas plataformas de e-comerce, como por exemplo mercado livre, carrefour, magalu entre outros. 

A utilização de dados é fundamental tanto para o monitoramento da saúde, quanto para a implementação e identificação de novas oportunidades para o negócio.

O processo a seguir se trata da criação de pipelines de extração de dados, bem como a transformação desses dados em insights significativos para o negócio. Utilizamos um processo desenvolvido em python que se conecta à API e em seguida coleta os dados e insere dentro de um banco de dados intermediário, o qual chamamos de stage. A partir dos dados armazenados no stage foi criado um datawrehouse utilizando o pentaho como orquestrador da ETL. Todo o processo é executado de hora em hora em um servidor Windows. Por fim, os dados são visualizados de maneira gráfica em um arquivo power BI.

O objetivo é atender a demanda do cliente que precisava visualizar as informações de modo que pudesse ser possível armazenar um histórico e substituir o sistema antigo.

![Untitled (8)](https://github.com/Jezandre/pequeno_encanto/assets/63671761/e6307623-3237-4a52-b5fe-c12937714c41)


## Materiais utilizados

A baixo segue a lista de aplicações que foi utilizada para o processo:

- Windows server
- Python Python 3.12.3
- Bibliotecas
    - import requests
    - base64
    - configparser
    - sqlalchemy create_engine
    - mysql.connector    
    - pandas
    - import StringIO
    - json
    - logging
    - smtplib
    - email.mime.text import MIMEText
    - requests
    - base64
    - configparser
    - sqlalchemy.sql import select
    - pandas as pd
    - logging
    - logging.handlers import TimedRotatingFileHandler
    - os
    - datetime
- MySQL 8.3
- Pentaho
- Power BI
- GIT 2.42.0.windows.2

Para a extração de dados foram desenvoldos arquivos em python para cada tabela. O arquivo functions.py foi desenvolvido em POO de maneira que cada arquivo de extração pudesse reutilizar o código.

Os arquivos utilizados são:
- Pastas:
    - logs: pasta para armazenar os dados de erros do processo de extração.
    - PBI: Pasta para armazenar os arquivos relacionados ao power BI
- Arquivos:
    - Extracao-API_<<Nome da tabela>>.py
    - functions.py

## Descrição dos arquivos

## functions.py

Este arquivo armazena todas as classes e funções utilizadas pelas aplicações extratoras. Cada classe está relacionada a uma função específica dentro do projeto. A baixo descreverei o processo, a função e o detalhamento do código.

## Classe CriarToken

A API do bling possui o sistema de autenticação que se baseia na utilização de dois tokens. Um token, que é o que chamamos de refresh token é utilizado para acesser o link da aplicação solicitar o token temporário. Esse token temporário é utilizado para realizar a autenticação no sistema. Na documentação do bling temos mais detalhes principalmente referente a quantidade de requisições por dias e o tempo de duração de cada token.

Para a obtenção desse token utilizamos três funções: nova_chave, refresh_access_token e Novo_Token.

O processo é basicamente o seguinte utilizamos o token obtido através do bling dentro de um arquivo, podendo ser ini ou json. Esse token solicita novos tokens e atualiza neste arquivo substituindo o refresh token e o token temporário anterior.


### nova_chave

A função nova chave é responsável por armazenar e sunstituir o valor dos tokens todas as vezes que essa função for solicitada.

```python
def nova_chave(chave, token, nova_variavel):
        # Caminho do arquivo INI
        arquivo_ini = 'infoDB.ini'
        # Crie um objeto ConfigParser e leia o arquivo INI
        config = configparser.ConfigParser()
        config.read(arquivo_ini)

        # Remova a variável ACCESS_TOKEN
        if f'{token}' in config[f'{chave}']:
            del config[f'{chave}'][f'{token}']

        # Defina a nova variável ACCESS_TOKEN    
        config[f'{chave}'][f'{token}'] = nova_variavel

        # Escreva as mudanças de volta no arquivo INI
        with open(arquivo_ini, 'w') as arquivo:
            config.write(arquivo)
```


1. **Definição da função:** A função `nova_chave` é definida para atualizar ou criar uma nova chave e valor em um arquivo INI. Ela aceita três parâmetros: `chave`, `token` e `nova_variavel`.

2. **Caminho do arquivo INI:** O caminho para o arquivo INI é definido como 'infoDB.ini'.

3. **Leitura do arquivo INI:** Um objeto `ConfigParser` é criado e usado para ler o arquivo INI especificado.

4. **Remoção da variável existente:** Verifica-se se a chave especificada (`chave`) e o token (`token`) existem no arquivo INI. Se existirem, o valor correspondente é removido.

5. **Definição da nova variável:** A nova variável (`nova_variavel`) é definida para a chave e token especificados no arquivo INI.

6. **Escrita das mudanças:** As mudanças são escritas de volta no arquivo INI. O arquivo é aberto em modo de escrita ('w') e as mudanças são escritas usando o método `write` do objeto `ConfigParser`.

Este código é útil para atualizar ou adicionar novas configurações em um arquivo INI, comumente usado para armazenar configurações de aplicativos.

### refresh_access_token

Essa função serve como renovação do token, ele faz a solicitação utilizando o token anterior e obtem os novos tokens refresh e o token temporário.

```python
def refresh_access_token(refresh_token, client_id, client_secret, token_url):
        # Codificar as credenciais do cliente para o cabeçalho Authorization
        credentials = f"{client_id}:{client_secret}"
        credentials_b64 = base64.b64encode(credentials.encode()).decode()

        # Parâmetros necessários para a solicitação de atualização do token
        payload = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': '1.0',
            'Authorization': f'Basic {credentials_b64}'
        }

        try:
            # Fazendo a solicitação POST para obter o novo Access Token
            response = requests.post(token_url, data=payload, headers=headers)
            response.raise_for_status()  # Lança uma exceção se a solicitação não for bem-sucedida

            # Retorna os dados da resposta (normalmente incluindo o novo Access Token)
            return response.json()
        except requests.exceptions.RequestException as e:
            # Lidar com erros de solicitação, como problemas de conexão ou resposta inválida
            print("Erro ao solicitar novo Access Token:", e)
            return None

```

1. **Codificação das credenciais do cliente:** As credenciais do cliente (ID do cliente e segredo do cliente) são combinadas em uma única string e codificadas em Base64 para autenticação com o servidor de autorização.

2. **Construção dos parâmetros da solicitação:** Os parâmetros necessários para a solicitação de atualização do token são definidos. Isso inclui o tipo de concessão (`grant_type`) como "refresh_token" e o próprio token de atualização (`refresh_token`).

3. **Configuração dos cabeçalhos da solicitação:** Os cabeçalhos da solicitação HTTP são configurados. O tipo de conteúdo (`Content-Type`) é definido como "application/x-www-form-urlencoded", a versão de aceitação (`Accept`) como "1.0", e o cabeçalho de autorização (`Authorization`) com as credenciais codificadas em Base64.

4. **Realização da solicitação POST:** Uma solicitação POST é feita para a URL de token especificada (`token_url`), enviando os parâmetros e cabeçalhos configurados anteriormente.

5. **Tratamento de exceções:** O código trata exceções de solicitação usando um bloco `try-except`, lidando com erros como problemas de conexão ou resposta inválida e imprimindo uma mensagem de erro se ocorrer uma exceção.

6. **Retorno dos dados da resposta:** Se a solicitação for bem-sucedida, os dados da resposta são retornados como um dicionário JSON, normalmente incluindo o novo token de acesso. Caso contrário, retorna `None`.

### Novo_Token

Essa função coleta as informações fornecidas pela função refresh_access_token e armazena dentro de um arquivo ini utilizando a função nova_chave.

```python
def Novo_Token():
        config = configparser.ConfigParser()
        config.read('infoDB.ini')

        # Exemplo de uso da função de atualização de token
        refresh_token = str(config'<<opções_personalisaveis>>')
        client_id = str(config['<<opções_personalisaveis>>'])
        client_secret = str(config['opções_personalisaveis'])
        token_url = 'https://www.bling.com.br/Api/v3/oauth/token?'


        new_tokens = CriarToken.refresh_access_token(refresh_token, client_id, client_secret, token_url)

        if new_tokens:
            # Use o novo Access Token
            access_token = new_tokens['access_token']
            CriarToken.nova_chave(chave='APICONECTIONS', token='access_token', nova_variavel=access_token)
            refresh_token = new_tokens['refresh_token']        
            CriarToken.nova_chave(chave='APICONECTIONS', token='refresh_token', nova_variavel=refresh_token)

        else:
            print("Não foi possível obter um novo Access Token.")

```

1. **Leitura das configurações:** Um objeto `ConfigParser` é criado e utilizado para ler as configurações de um arquivo INI.

2. **Obtenção das configurações:** As configurações necessárias para a atualização do token são lidas do arquivo INI. Isso inclui o `refresh_token`, `client_id`, `client_secret`, e `token_url`.

3. **Solicitação de novo token:** A função `refresh_access_token` é chamada com as configurações lidas do arquivo INI para obter um novo token de acesso.

4. **Atualização dos tokens no arquivo INI:** Se um novo token de acesso for obtido com sucesso, os tokens são armazenados no arquivo INI usando a função `nova_chave` do módulo `CriarToken`. Isso atualiza os valores de `access_token` e `refresh_token` no arquivo INI.

5. **Impressão de mensagem de erro:** Se não for possível obter um novo token de acesso, uma mensagem de erro é impressa.

## Classe Authenticator

A classe Authenticator será a responsável por fornecer os acessos às informações da API. Ela utilizará o token gerado anteriormente e fará a requisição dos dados. E esses serão os dados que utilizaremos para armazenar nos nossos stages e datawarehouse.

### authenticator

```python
def authenticator(caminho, parametro):
        # Lê arquivo com as credenciais
        config = configparser.ConfigParser()
        config.read('<<nomedoarquivo.ini>>')

        # Conecta com a API
        url = f"https://www.bling.com.br/Api/v3/{caminho}{parametro}"        
        access_token = str(config'<<opção_editavel>>')        
        client_id = str(config'<<opção_editavel>>')        
        client_secret = str(config'<<opção_editavel>>')       

        # Codificar as credenciais do client_app em base64
        credentials = f"{client_id}:{client_secret}"
        credentials_b64 = base64.b64encode(credentials.encode()).decode()

        headers = {
            "Authorization": f"Basic {credentials_b64}, Bearer {access_token}"
        }

        # response = requests.get(url, params=params, headers=headers)
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            # print(f"Resposta da API: parametro {parametro}")
            return data
        else:
            print("Falha ao fazer a requisição. Código de status:", response.status_code)
```

1. **Leitura das configurações:** Um objeto `ConfigParser` é criado e utilizado para ler as configurações de um arquivo INI (`infoDB.ini`).

2. **Conexão com a API:** A URL para a API é construída com base no caminho (`caminho`) e parâmetro (`parametro`) fornecidos como argumentos da função. As credenciais de acesso (`access_token`), `client_id`, `client_secret` e `state` são obtidas do arquivo INI.

3. **Codificação das credenciais:** As credenciais do cliente (`client_id` e `client_secret`) são combinadas em uma única string e codificadas em Base64 para autenticação com o servidor de autorização.

4. **Configuração dos cabeçalhos da solicitação:** Os cabeçalhos da solicitação HTTP são configurados, incluindo a autorização básica usando as credenciais codificadas em Base64 e o token de acesso (`access_token`) no formato Bearer.

5. **Realização da solicitação GET:** Uma solicitação GET é feita para a URL da API especificada, incluindo os cabeçalhos configurados anteriormente.

6. **Verificação do status da resposta:** Se a resposta da API tiver um código de status 200 (OK), os dados da resposta são retornados como um objeto JSON. Caso contrário, uma mensagem de erro é impressa com o código de status da resposta.

## Classe CapturarDados

A função CapturarDados é responsavel por obter os dados no formato json e transformá-los em um dataframe do pandas. Nesse dataframe poderemos trabalhar algumas transformações antes de inserir os dados na base de dados. Esta uma etapa pode 

### captardadosAPI

```python
def captardaosAPI(caminho, pagina):
        df1 = pd.DataFrame()     

        # Puxar os dados
        data = Authenticator.authenticator(caminho=caminho,  parametro=f'?pagina={pagina}')

        # As paginas serão navegadas até a página que não possuir dados e criará o data frame
        while bool(data['data']) == True:
            # print(bool(data['data']))     
            data = Authenticator.authenticator(caminho=caminho, parametro=f'?pagina={pagina}')
            pagina = pagina + 1      
            json_string = StringIO(json.dumps(data['data'])).getvalue()      
            df2 = pd.json_normalize(json.loads(json_string))      
            df1 = pd.concat([df1,df2], ignore_index=True)

        return df1
```


1. **Criação do DataFrame vazio:** Um DataFrame vazio (`df1`) é inicializado usando a biblioteca pandas.

2. **Puxar os dados da API:** A função `authenticator` é chamada para autenticar uma requisição à API usando o caminho (`caminho`) e a página (`pagina`) fornecidos como argumentos da função. Os dados da resposta da API são armazenados na variável `data`.

3. **Navegação pelas páginas:** Enquanto a chave 'data' no JSON retornado da API estiver vazia, o código continuará navegando para a próxima página. A cada iteração, a página é incrementada e uma nova solicitação à API é feita.

4. **Conversão dos dados em DataFrame:** Os dados JSON retornados pela API são convertidos em uma string JSON, normalizada com `pd.json_normalize()` para criar um DataFrame pandas (`df2`). Este DataFrame é então concatenado com o DataFrame principal (`df1`) usando `pd.concat()`.

5. **Retorno do DataFrame final:** Após percorrer todas as páginas e coletar todos os dados, o DataFrame final (`df1`) contendo todos os dados da API é retornado.

Este código é útil para extrair dados de APIs paginadas e armazená-los em um formato tabular para análise posterior usando a biblioteca pandas.

## Classe InserirDados

A classe inserir dados é utilizada para o processo de inserção das informações dentro de um banco de dados. Ele utiliza o SQL Alchemy como injestor. 

Os dados podem ser inseridos alterando o comando, porém é importante lembrar, o comando replace os dados são todos substituídos sem a necessidade da criação da tabela no banco de dados. Já o comando append precisa que a tabela e os esquemas já tenham sido definidos. É importante ressaltar que o esquema intefere diretamente na performance do banco de dados. Nesse caso a injestão de dados pelo SQL Alchemy sem a criação do schema não fará tanta diferença devido a quantidade de dados não ser grande.

#### inserirDadosMysql

```python
def inserirDadosMysql(df, tabela, comando):

        # Lê arquivo com as credenciais
        config = configparser.ConfigParser()
        config.read('infoDB.ini')

        # Conexão bd 
        usuario = str(config'<<user>>')  
        senha = str(config'<<senha>>')  
        host = str(config'<<host>>')  
        nome_do_banco = str(config'<<nomedobanco>>')  

        # Configurando Engine do MySQL
        engine = create_engine(f'mysql+mysqlconnector://{usuario}:{senha}@{host}/{nome_do_banco}')        

        # Comando criar tabela, atualizar e inserir registros na tabela
        df.to_sql(name=tabela, con=engine, if_exists=comando, index=False)
        con = engine.connect()
        con.close()

        print(f'Dados inseridos com sucesso na tabela {tabela}')

```
1. **Leitura das configurações:** Um objeto `ConfigParser` é criado e utilizado para ler as configurações de um arquivo INI (`infoDB.ini`).

2. **Configuração da conexão com o banco de dados:** As configurações necessárias para a conexão com o banco de dados MySQL são lidas do arquivo INI. Isso inclui o nome de usuário (`user`), senha (`password`), host (`host`) e nome do banco de dados (`database`).

3. **Configuração da engine do MySQL:** Uma engine do SQLAlchemy é criada usando as informações de conexão lidas do arquivo INI.

4. **Inserção de dados na tabela:** Os dados do DataFrame (`df`) são inseridos na tabela MySQL especificada (`tabela`). O parâmetro `comando` determina o que fazer se a tabela já existir (`'replace'` para substituir, `'append'` para adicionar novos registros e `'fail'` para falhar se a tabela já existir).

5. **Fechamento da conexão:** A conexão com o banco de dados é fechada após a inserção dos dados.

6. **Mensagem de sucesso:** Uma mensagem é impressa indicando que os dados foram inseridos com sucesso na tabela especificada.

## Classe EnviarEmail

A classe enviar email foi criada com o intuíto de alertar sobre erros do processo utilizando configurações SMTP. Ela utiliza uma conta do gmail e identifica qual erro e qual arquivo teve problema. Dessa maneira é possível identificar qualquer tipo de problema relacionado ao processo de extração.

### enviar_email_log

```python
config = configparser.ConfigParser()
        config.read('<<nomedoarquivo>>.ini')

        # Configurações de e-mail
        remetente = '<<email_do_remetente>>'
        destinatario = '<<email_do_destinatario>>'
        assunto = f'Erro na aplicação de extração: {nome_do_arquivo}'
        senha = str(config'<<opções_editaveis>>')
        corpo = f'Ocorreu um erro ao executar o código de extração:\n\nNome do arquivo: {nome_do_arquivo}\n\nErro: {mensagem}'

        # Configuração do e-mail
        msg = MIMEMultipart()
        msg['From'] = remetente
        msg['To'] = destinatario
        msg['Subject'] = assunto
        msg.attach(MIMEText(corpo, 'plain'))

        # Envio do e-mail
        try:
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(remetente, senha)
            texto = msg.as_string()
            server.sendmail(remetente, destinatario, texto)
            server.quit()
            logging.info('E-mail de log enviado com sucesso!')
        except Exception as e:
            logging.error(f"Erro ao enviar e-mail de log: {str(e)}")

```

1. **Leitura das configurações:** Um objeto `ConfigParser` é criado e utilizado para ler as configurações de um arquivo INI (`infoDB.ini`). As configurações de e-mail, incluindo o endereço do remetente, destinatário, senha e assunto do e-mail são lidas do arquivo INI.

2. **Configuração da mensagem de e-mail:** São configurados o remetente (`remetente`), destinatário (`destinatario`), assunto (`assunto`) e corpo (`corpo`) do e-mail. O corpo do e-mail inclui detalhes sobre o erro, como o nome do arquivo que causou o erro (`nome_do_arquivo`) e a mensagem de erro (`mensagem`).

3. **Configuração do e-mail:** Um objeto `MIMEMultipart` é criado para representar a mensagem de e-mail. São configurados o remetente, destinatário, assunto e corpo do e-mail usando as informações definidas anteriormente.

4. **Envio do e-mail:** Uma tentativa é feita para enviar o e-mail usando o servidor SMTP do Gmail (`smtp.gmail.com` na porta `587`). O servidor é configurado para iniciar uma conexão segura (`starttls()`) e fazer login usando o endereço de e-mail do remetente e senha. A mensagem é enviada usando `sendmail()` e a conexão é encerrada (`quit()`). Se ocorrer um erro durante o envio do e-mail, uma mensagem de erro é registrada no log.

## Classe SalvarLogs

A classe SalvarLogs assim como a classe enviar e-mails serve para armazenar informações de erros da operação. Ela levanta as informções geradas e armazena dentro de um arquivo de log.

### configurar_logger

```python
def configurar_logger(nome_arquivo, nivel=logging.DEBUG):
        """Configura o logger para direcionar saída para um arquivo com rotação de logs."""
        # Configuração do logger
        logger = logging.getLogger()
        logger.setLevel(nivel)
        
        # Definindo o manipulador de arquivos com rotação de logs
        handler = TimedRotatingFileHandler(nome_arquivo, when="midnight", interval=1, backupCount=7)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # Adicionando o manipulador ao logger
        logger.addHandler(handler)
```

1. **Definição da função:** A função `configurar_logger` é definida para configurar o logger. Ela aceita dois parâmetros: `nome_arquivo` (o nome do arquivo de log) e `nivel` (o nível de logging, que padrão é `logging.DEBUG`).

2. **Configuração do logger:** O logger raiz é obtido usando `logging.getLogger()` e seu nível é definido para o valor especificado em `nivel`.

3. **Definição do manipulador de arquivos:** Um manipulador `TimedRotatingFileHandler` é criado para lidar com a saída do logger. Este manipulador cria arquivos de log com rotação em intervalos especificados (neste caso, à meia-noite diariamente) e mantém um número limitado de arquivos de backup.

4. **Formatação do log:** É definido um formato para as mensagens de log, que inclui a data e hora em que a mensagem foi registrada, o nível do log e a própria mensagem.

5. **Adição do manipulador ao logger:** O manipulador criado é adicionado ao logger raiz para direcionar a saída de log para o arquivo especificado.

## Classe QueryBancoDados

Essa classe possui três funções muito úteis para realizar consultas dentro de um banco de dados. Nesse projeto a principal função é para a utilização de atualização de dados de maneira incremental. 

O bing possui pelo menos duas maneiras de localizar os dados, um por paginação e o outro por id. Em geral os dados obtidos atavés de id são detalhes de alguma informação obtido na paginação. E para cada consulta de id é um requisição então, neste caso optamos por utilizar atualização incremental baseada na id. Fizemos isso para otimizar mais o processo de maneira mais simples.

### conectarBancodeDados
```python
def conectarBancodeDados():
        # Conectar ao banco de dados MySQL

        # Lê arquivo com as credenciais
        config = configparser.ConfigParser()
        config.read('<<nomedoarquivo>>.ini')

        # Conexão bd 
        usuario = str(config<<'opçãopersonalizada'>>)  
        senha = str(config'<<opçãopersonalizada>>')  
        host = str(config'<<opçãopersonalizada>>')  
        nome_do_banco = str(config'<<opçãopersonalizada>>')

        conexao = mysql.connector.connect(
            host=host,
            user=usuario,
            password=senha,
            database=nome_do_banco
        )
        return conexao
```
1. **Leitura das configurações:** Um objeto `ConfigParser` é criado e utilizado para ler as configurações de um arquivo INI (`infoDB.ini`). As configurações de conexão com o banco de dados MySQL, incluindo nome de usuário (`user`), senha (`password`), host (`host`) e nome do banco de dados (`database`), são lidas do arquivo INI.

2. **Conexão ao banco de dados:** Uma conexão é estabelecida com o banco de dados MySQL utilizando as informações de conexão obtidas do arquivo INI. Isso é feito usando a função `mysql.connector.connect()` do conector MySQL para Python.

3. **Retorno da conexão:** A conexão estabelecida é retornada pela função para ser usada em operações subsequentes.


### compararDados

```python
    def compararDados(tabela, nome_coluna):        
        conexao = QueryBancoDados.conectarBancodeDados()
        
        # Definir a consulta SQL
        query = f"SELECT {nome_coluna} FROM {tabela}"

        # Executar a consulta e obter os resultados
        df = pd.read_sql(query, con=conexao)

        # Fechar a conexão
        conexao.close()

        # Exibir o DataFrame
        return df
```

1. **Conexão ao banco de dados:** A função `conectarBancodeDados` do módulo `QueryBancoDados` é chamada para estabelecer uma conexão com o banco de dados MySQL.

2. **Definição da consulta SQL:** Uma consulta SQL é construída para selecionar os dados da coluna especificada (`nome_coluna`) da tabela especificada (`tabela`).

3. **Execução da consulta e obtenção dos resultados:** A consulta SQL é executada usando a função `pd.read_sql()` do pandas, que lê os resultados da consulta diretamente em um DataFrame pandas (`df`).

4. **Fechamento da conexão:** Após obter os dados, a conexão com o banco de dados é fechada para liberar recursos.

5. **Retorno do DataFrame:** O DataFrame contendo os dados da coluna selecionada é retornado pela função para análise ou processamento posterior.

### truncate

```python
  def truncate(tabela):
        conexao = QueryBancoDados.conectarBancodeDados()

        # Definir a consulta SQL
        query = f"TRUNCATE TABLE {tabela}"

        # Executar a consulta e obter os resultados
        df = pd.read_sql(query, con=conexao)

        # Fechar a conexão
        conexao.close()

        # Exibir o DataFrame
        return df

```
1. **Conexão ao banco de dados:** A função `conectarBancodeDados` do módulo `QueryBancoDados` é chamada para estabelecer uma conexão com o banco de dados MySQL.

2. **Definição da consulta SQL:** Uma consulta SQL é construída para truncar a tabela especificada (`tabela`), removendo todos os seus registros.

3. **Execução da consulta:** A consulta SQL é executada no banco de dados usando a função `pd.read_sql()`, embora neste caso a consulta não retorne dados, apenas realiza uma operação no banco de dados.

4. **Fechamento da conexão:** Após a execução da operação, a conexão com o banco de dados é fechada para liberar recursos.

5. **Retorno do DataFrame:** Como a operação de truncamento não retorna dados, o DataFrame retornado é vazio.

## Extracao-API_<<Nome da tabela>>.py

O processo foi separado por arquivos para cada tabela construída específicamente. Isso auxilia na organização e na manutenção em caso de erros. Porém a estrutura para algumas extrações básicas se repete quando as informações estão em formato paginado. 

Em alguns casos em que era preciso mais detalhes, é adicionada uma função no próprio arquivo para a coleta deste dados específicos.

### Estrutura base

```python
from functions import *
import datetime
import pandas as pd
import os

def main():
   # Função para criar um novo refresh token
   CriarToken.Novo_Token()

   # Função para capturar dados
   df1 = CapturarDados.captardaosAPI(caminho='<<campopersonalizado>>', pagina=1)

   # Adicionar data carga
   df1['Data_Carga'] = pd.Timestamp.now()

   print(df1)

   InserirDados.inserirDadosMysql(df=df1, tabela="stg_categoria", comando='replace')

if __name__ == "__main__":
   # Definindo o nome do arquivo para salvar nos log
   nome_arquivo = os.path.basename(__file__)
   
   print("Horário de início:", datetime.datetime.now())
   
   try:
      main()      
   except Exception as e:
      # Salvando logs na maquina caso erro
      logs= SalvarLogs.configurar_logger(nome_arquivo=f'logs/{nome_arquivo}_logfile.log')

      # Captura a exceção e registra o log
      logging.error(f"Ocorreu um erro: {str(e)}")

      # Envio de e-mail com o log
      EnviarEmail.enviar_email_log(str(e), nome_arquivo)   
   
   print("Horário de fim:", datetime.datetime.now())
```

1. **Importação de Módulos:**
   - O script importa os seguintes módulos e bibliotecas:
     - `functions`: Módulo contendo as funções auxiliares para criar um novo refresh token, capturar dados da API, inserir dados no banco de dados MySQL, gerenciar logs e enviar e-mails.
     - `datetime`: Biblioteca padrão do Python para manipulação de datas e horas.
     - `pandas`: Biblioteca para manipulação e análise de dados.
     - `os`: Biblioteca padrão do Python para interagir com o sistema operacional.

2. **Definição da Função Principal (`main`):**
   - A função `main` é definida para coordenar a execução das tarefas principais do script.

3. **Criação de um Novo Refresh Token:**
   - Utiliza a função `Novo_Token` do módulo `CriarToken` para criar um novo refresh token.

4. **Captura de Dados da API:**
   - Utiliza a função `captardaosAPI` do módulo `CapturarDados` para capturar dados da API.
   - Os dados são armazenados em um DataFrame pandas (`df1`).

5. **Adição da Data de Carga aos Dados:**
   - Uma coluna chamada `Data_Carga` é adicionada ao DataFrame `df1`, contendo o timestamp atual.

6. **Inserção dos Dados no Banco de Dados MySQL:**
   - Utiliza a função `inserirDadosMysql` do módulo `InserirDados` para inserir os dados do DataFrame `df1` na tabela `stg_categoria` do banco de dados MySQL.
   - Utiliza o parâmetro `comando='replace'` para substituir os dados existentes na tabela.

7. **Gestão de Erros:**
   - Utiliza o bloco `try-except` para capturar e lidar com exceções que podem ocorrer durante a execução do script.
   - Se ocorrer uma exceção, registra um log do erro usando a função `configurar_logger` do módulo `SalvarLogs`.
   - Envia um e-mail contendo o log do erro para um endereço de e-mail especificado usando a função `enviar_email_log` do módulo `EnviarEmail`.

8. **Dependências:**
- `functions.py`: Módulo contendo as funções auxiliares para criar um novo refresh token, capturar dados da API, inserir dados no banco de dados MySQL, gerenciar logs e enviar e-mails.
- `datetime`: Biblioteca padrão do Python para manipulação de datas e horas.
- `pandas`: Biblioteca para manipulação e análise de dados.
- `os`: Biblioteca padrão do Python para interagir com o sistema operacional.

### Estrutura de atualização incremental

Nos casos em que é preciso obter mais detalhes das informações foram adicionados uma função e as seguintes linhas na função main. Isso foi feito com o intuito de diminuir as requisições feitas na API.

```python

# Capturar dados
   df1 = CapturarDados.captardaosAPI(caminho=caminho, pagina=1)

    # Fazendo a query no banco
   df_idregistradonobanco = QueryBancoDados.compararDados(tabela="<<nomedatabela>>s", nome_coluna = 'id')

    # Comparando dataframes
   ids_nao_em_df2 = df1[~df1['id'].isin(df_idregistradonobanco['id'])]
#    print(ids_nao_em_df2)

   # Definir dataframe de frete e juntar com o dataframe de pedidos
   df_detalhes = pegarDetalhes(df=ids_nao_em_df2, caminho=caminho) 

```

1. **Captura de Dados:**
   - Utiliza a função `captardaosAPI` do módulo `CapturarDados` para capturar dados da API.
   - A função é chamada com os seguintes parâmetros:
     - `caminho`: Caminho da API a ser acessada.
     - `pagina`: Página de onde começar a captura dos dados.

2. **Consulta no Banco de Dados:**
   - Utiliza a função `compararDados` do módulo `QueryBancoDados` para fazer uma consulta no banco de dados e obter dados de uma tabela específica (`stg_pedidos_detalhes`).
   - A função é chamada com os seguintes parâmetros:
     - `tabela`: Nome da tabela no banco de dados.
     - `nome_coluna`: Nome da coluna a ser consultada.

3. **Comparação de DataFrames:**
   - Os DataFrames `df1` (dados capturados da API) e `df_idregistradonobanco` (dados consultados no banco de dados) são comparados para identificar registros que estão em `df1` mas não em `df_idregistradonobanco`.
   - Os registros que não estão em `df_idregistradonobanco` são armazenados em `ids_nao_em_df2`.

4. **Definição de DataFrame de Detalhes:**
   - Utiliza a função `pegarDetalhes` para obter detalhes adicionais dos registros identificados em `ids_nao_em_df2` na API.
   - O DataFrame resultante é armazenado em `df_detalhes`.

### pegarDetalhes

```python
def pegarDetalhes(df, caminho):
    # QueryBancoDados.truncate(tabela='stg_pedidos_detalhes')
    df2 = pd.DataFrame()
    count = 0  # Variável de contagem de iterações
    if not df.empty:
        lista_dados = df['id'].tolist()

        for id_pedido in lista_dados:
            # Pegar detalhes do pedido
            data = Authenticator.authenticator(caminho=caminho, parametro=f'/{id_pedido}')
            novo_registro = {
                'id': int(id_pedido),
                'valorFrete': data['data']['<<campo_de_detalhe>>']                
            }
            # Adicionar o registro ao DataFrame usando o método append()
            df2 = df2._append(novo_registro, ignore_index=True)
            
            print(df2)

            # Incrementar a contagem de iterações
            count += 1

            # Se a contagem atingir 100, inserir os dados no banco de dados e reiniciar a contagem
            if count == 100:
                df2['Data_Carga'] = pd.Timestamp.now()
                InserirDados.inserirDadosMysql(df=df2, tabela="<<nome_tabela>>", comando='append')
                df2 = pd.DataFrame()  # Reinicia o DataFrame                
                count = 0


        if not df2.empty:
            df2['Data_Carga'] = pd.Timestamp.now()
            InserirDados.inserirDadosMysql(df=df2, tabela="<<nome_tabela>>", comando='append')               

    else:
        print('Não existem novos registros')

```

Esta função é responsável por obter detalhes adicionais dos registros capturados da API. Ela recebe um DataFrame contendo os registros a serem detalhados e o caminho da API a ser acessada.

### Parâmetros:
- `df`: DataFrame contendo os registros a serem detalhados.
- `caminho`: Caminho da API a ser acessada para obter os detalhes dos registros.

### Funcionamento:
1. **Inicialização de Variáveis:**
   - Cria um DataFrame vazio `df2` para armazenar os detalhes dos registros.
   - Inicializa a variável `count` para contar o número de iterações.

2. **Verificação se o DataFrame está Vazio:**
   - Verifica se o DataFrame `df` não está vazio.

3. **Iteração sobre os Registros no DataFrame:**
   - Itera sobre cada registro no DataFrame `df`.

4. **Captura de Detalhes da API:**
   - Para cada registro, faz uma chamada à API utilizando o caminho fornecido e o ID do pedido.
   - Captura detalhes.

5. **Adição dos Detalhes ao DataFrame:**
   - Adiciona os detalhes capturados como novas linhas ao DataFrame `df2` utilizando o método `_append()`.

6. **Inserção dos Dados no Banco de Dados:**
   - Se o número de registros no DataFrame `df2` atingir 100, insere os dados no banco de dados utilizando a função `inserirDadosMysql`.
   - Reinicia o DataFrame `df2` e a variável `count` após inserir os dados no banco de dados.

7. **Última Inserção dos Dados no Banco de Dados:**
   - Após a iteração, se o DataFrame `df2` não estiver vazio, insere os dados restantes no banco de dados.

8. **Mensagem de Não Existência de Novos Registros:**
   - Se o DataFrame `df` estiver vazio, imprime uma mensagem indicando que não existem novos registros.

