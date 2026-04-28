import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os
import json
import boto3
from botocore.exceptions import ClientError
import logging
from io import StringIO
from datetime import datetime, timedelta, date, time

print("Iniciando ETL...")

# Importando Bibliotecas Necessárias:
# pandas as pd = Estrutura e manipulação de dados (DataFrame) e exportação para CSV;
# mysql.connector = Conexão com Banco de dados para validações;
# dotenv = Criação de variaveis de ambiente
# OS = Funções para checar existência de pastas/arquivos;
# json = Criar arquivos .json para melhor manipulação no Front;
# boto3 = Interagir com a AWS (s3);
# logging = Debug e registrar erros;
# stringIO = Cria arquivos na memoria para enviar ao S3;
# datetime = Transformar em horas e pegar diferenças com mais facilidade.

# Arquivo de controle para saber o momento de criar o Client

controleArquivo = "./controle.txt"

# Criando espaço para CSV na mémoria
buffer_trusted = StringIO()
buffer_client = StringIO()

# Carregando as variaveis de ambiente
load_dotenv()

print("Variáveis de ambiente carregadas")

# Colunas de status dos Monitores
status_cols = [
    "bpm_status", "pa_status", "spo2_status", "resp_status",
    "temperatura_status", "pic_status", "pvc_status",
    "ecg_status", "etco2_status"
]

# Salvando bucket como variavel
bucket = os.getenv("bucket")

# Conectando com S3:
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
    aws_session_token=os.getenv("aws_session_token")
)

print("Conectando ao S3...")

# Criando Paginator para ler bucket da S3

print("Buscando arquivos RAW no S3...")

paginator = s3.get_paginator('list_objects_v2')
registros = []

for page in paginator.paginate(Bucket = bucket,Prefix = "raw/"):
    
    for obj in page["Contents"]:

        chave = obj["Key"]

        response = s3.get_object(Bucket= bucket, Key=chave)
        registros.append({"conteudo": response})

registros = sorted(registros, key = lambda x: x["conteudo"]["LastModified"], reverse=True)

raw = registros[0]["conteudo"]['Body'].read().decode('utf-8')

print("RAW carregado com sucesso")

# Conectando com MYSQL:
def conectar():
    print("Conectando ao MySQL...")
    try:
        conn = mysql.connector.connect(
                host=os.getenv("host"),
                user=os.getenv("user"),
                password=os.getenv("password"),
                database=os.getenv("database"))
        
        if conn.is_connected():
            print("Conectado ao SQL com Sucesso")
            return conn
        
    except mysql.connector.Error as e:
        print(f"Erro: {e}")

# Função para buscar os limites dos monitores
def buscar_limites(cursor, id_monitor):

    # Select para pegar os limites de acordo com o componente
    query = """
        SELECT c.nome_componente, cm.limite
        FROM componente_monitor cm
        JOIN componentes c 
            ON cm.fk_componente = c.id_componente
        WHERE cm.fk_monitor = %s
    """

    # Cursor para executar a Query
    cursor.execute(query, (id_monitor,))
    # Resultado da Query salvo
    resultado = cursor.fetchall()

    # Transformando a query em Dicionario
    limites = {}
    for nome, limite in resultado:
        limites[nome.lower()] = float(limite)

    return limites

# Função para ler o arquivo de controle
def ler_controle():
    if not os.path.exists(controleArquivo):
        return 0
    with open(controleArquivo, "r") as f:
        return int(f.read())

# Função para salvar o arquivo de controle
def salvar_controle(valor):
    with open(controleArquivo, "w") as f:
        f.write(str(valor))

# Função para preparar raw para tratamento
def preparar_raw(df):
    # Copiando dataframe para manipulação
    df = df.copy()

    # Convertendo datas
    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        format="%d-%m-%Y %H_%M_%S",
        errors="coerce"
    )

    # Limpando possiveis dados invalidos e ordenando por monitor e data em ordem cronologica
    df = df.dropna(subset=["timestamp"])
    df = df.sort_values(["id_monitor", "timestamp"])

    return df

# Função para tratar os dados (trusted)
def trusted(df):
    print("Processando camada TRUSTED...")
    # Utiliza o preparar_raw para deixar os dados prontos para tratamento
    df = preparar_raw(df)

    # Pega os ultimos 10 registros (10 minutos)
    df_last = df.tail(10)

    # Checa se tem dados
    if df_last.empty:
        print("Sem dados para trusted")
        return

    # Pega o pico de cada coisa dos ultimos 10minutos
    horarioInicio = df_last["timestamp"].min()
    horarioFim = df_last["timestamp"].max()
    idMonitor = df_last["id_monitor"].iloc[-1]

    maxCPU = df_last["cpu_percent"].max()
    maxRAM = df_last["ram_percent"].max()
    maxDISK = df_last["disk_usage_percent"].max()

    mincpuporcentagem = df_last["cpu_percent"].min()
    minramporcentagem = df_last["ram_percent"].min()
    ultimacapturacpu = df_last["cpu_percent"].iloc[-1]
    ultimacapturaram = df_last["ram_percent"].iloc[-1]

    somaRede = ((df_last["bytes_sent_per_sec"] +
             df_last["bytes_recv_per_sec"]) * 8 / 1_000_000).round(2)

    minredeMBS = somaRede.min()
    ultimacapturarede = somaRede.iloc[-1]
    bandaLarga = somaRede.max()

    kpi_rede_zero = (somaRede <= 0.01).sum()

    # Pega o ultimo registro do DF
    ultimo = df_last.iloc[-1]

    # Monta dicionario de Registros
    registro = {
        "idMonitor": idMonitor,
        "horarioInicio": horarioInicio,
        "horarioFim": horarioFim,
        "maxCPU": maxCPU,
        "mincpuporcentagem": mincpuporcentagem,
        "minramporcentagem": minramporcentagem,
        "ultimacapturacpu": ultimacapturacpu,
        "ultimacapturaram": ultimacapturaram,
        "maxRAM": maxRAM,
        "maxDISK": maxDISK,
        "minredeMBS": minredeMBS,
        "ultimacapturarede": ultimacapturarede,
        "bandaLarga": bandaLarga,
        "kpi_rede_zero": kpi_rede_zero
    }

    # Adiciona os Status dinamicamente com for
    for col in status_cols:
        registro[col] = ultimo[col]

    # Cria dataframe
    temp_trusted = pd.DataFrame([registro])

    # Baixando Trusted do S3 caso exista para incrementar e enviar
    try:
        response = s3.get_object(Bucket=bucket, Key='trusted/trusted.csv')
        conteudo = response['Body'].read().decode('utf-8')
        df_existente = pd.read_csv(StringIO(conteudo))

        df_trusted = pd.concat([df_existente, temp_trusted], ignore_index=True)

    except s3.exceptions.NoSuchKey:
        # Primeira execução
        df_trusted = temp_trusted

    buffer = StringIO()
    df_trusted.to_csv(buffer, index=False)

    s3.put_object(
        Bucket=bucket,
        Key='trusted/trusted.csv',
        Body=buffer.getvalue()
    )

    print("Trusted atualizado no S3")

    return df_trusted

# Função para deixar os dados para o Cliente
def client(df, cursor):
    print("Processando camada Client...")
    # Pega os ultimos 3 registros do trusted
    df_client = df.tail(3)

    # Garante que o Trusted tem dados suficientes
    if df_client.empty:
        print("Sem dados")
        return

    resultado = []

    # Percorrer o df_trusted
    for _, row in df_client.iterrows():
        # Pega o id do monitor da linha e transforma em int
        id_monitor = int(row["idMonitor"])
        # intervalo = ((datetime.strptime(str(row["horarioFim"]), "%Y-%m-%d %H:%M:%S").date()) - (datetime.strptime(str(row["horarioInicio"]), "%Y-%m-%d %H:%M:%S").date())).total_seconds()
        intervalo = round(((datetime.strptime(str(row["horarioFim"]), "%Y-%m-%d %H:%M:%S")) - (datetime.strptime(str(row["horarioInicio"]), "%Y-%m-%d %H:%M:%S"))).total_seconds() / 60, 2)
        # intervalo = (datetime.strptime(str(row["horarioFim"]), "%Y-%m-%d %H:%M:%S"))
        horarioInicio = str(row["horarioInicio"])
        horarioFim = str(row["horarioFim"])

        # Busca os limites do banco com base no id do monitor e o cursor informado anteriormente
        limites = buscar_limites(cursor, id_monitor)

        # Cria as colunas que irão para o cliente
        cpu = row["maxCPU"]
        ram = row["maxRAM"]
        disk = row["maxDISK"]
        rede = row["bandaLarga"]
        mincpu = row["mincpuporcentagem"]
        minram = row["minramporcentagem"]
        ultcpu = row["ultimacapturacpu"]
        ultram = row["ultimacapturaram"]
        minrede = row["minredeMBS"]
        ultrede = row["ultimacapturarede"]

        # Função para definir o status do monitor
        def status(valor, limite):
            if valor <= limite:
                return "OK"
            elif valor <= limite * 1.2:
                return "Alerta"
            return "Crítico"
        
        # Procura os limites de cada componente no banco
        limite_cpu = limites.get("cpu")
        limite_ram = limites.get("ram")
        limite_disk = limites.get("disco")
        limite_rede = limites.get("rede")

        statuscpu = status(cpu, limite_cpu)
        statusram = status(ram, limite_ram)
        statusdisco = status(disk, limite_disk)
        statusrede = status(rede, limite_rede)

        # Gerá quantidade de modulos ativos e se o monitor está ativo
        qtd_modulos_ativos = sum(row[col] == "Ativo" for col in status_cols)
        monitor_ativo = qtd_modulos_ativos > 0
        kpi_rede_zero = int(row["kpi_rede_zero"])

        # Cria status geral com base em como está cada componente
        if "Crítico" in [statuscpu, statusram, statusdisco, statusrede]:
            statusgeral = "Crítico"
        elif "Alerta" in [statuscpu, statusram, statusdisco, statusrede]:
            statusgeral = "Alerta"
        else:
            statusgeral = "OK"

        # Cria a base do json de resultado
        resultado.append({
            "idmonitor": id_monitor,
            "horarioInicio": horarioInicio,
            "HorarioFim": horarioFim,
            "intervalomin": intervalo,
            "picocpuporcentagem": cpu,
            "picoramporcentagem": ram,
            "picodiscoporcentagem": disk,
            "mincpuporcentagem": mincpu,
            "minramporcentagem": minram,
            "ultimacapturacpu": ultcpu,
            "ultimacapturaram": ultram,
            "minredeMBS": minrede,
            "ultimacapturarede": ultrede,
            "redeMBS": rede,
            "statusgeral": statusgeral,
            "statusram": statusram,
            "statuscpu": statuscpu,
            "statusdisco": statusdisco,
            "statusrede": statusrede,
            "monitorativo": monitor_ativo,
            "qtdmodulosativos": qtd_modulos_ativos,
            "kpi_rede_zero": kpi_rede_zero,
            "modulos": {
            col: row[col] for col in status_cols
            },

        })

    # Vai para o começo do Buffer e Limpa
    buffer_client.seek(0)
    buffer_client.truncate(0)

    # Cria o Json
    json.dump(resultado, buffer_client, indent=4, ensure_ascii=False)

    # Baixando o Client do S3 caso exista para incrementar e enviar
    try:
        response = s3.get_object(Bucket=bucket, Key='client/client.json')
        conteudo = response['Body'].read().decode('utf-8')
        df_existente = pd.read_json(StringIO(conteudo))
        json_antigo = json.loads(conteudo)
        json_novo = json.loads(buffer_client.getvalue())
        jsonFinal = json.dumps(json_antigo + json_novo, indent=4, ensure_ascii=False)

    except s3.exceptions.NoSuchKey:
            # Primeira execução
            jsonFinal = buffer_client.getvalue()


    s3.put_object(Bucket=bucket, Key='client/client.json', Body=str(jsonFinal))

    print("Client atualizado no S3")

    return resultado

def main():
    # Lendo arquivo raw
    df_raw = pd.read_csv(StringIO(raw))

    # Alertando caso não tenha encontrado registro no arquivo
    if df_raw.empty:
        print("RAW vazio")
        return
    
    # Criando o Dataframe Trusted
    df_trusted = trusted(df_raw)

    # Validação de 3 novas linhas no trusted para 1 client novo
    total_atual = len(df_trusted)
    total_anterior = ler_controle()

    print(f"Controle -> atual: {total_atual} - anterior: {total_anterior}")

    # Criando cliente caso passe na condição
    if total_atual - total_anterior >= 3:
        # Pegando cursor do MYSQl para poder executar queries
        conn = conectar()
        cursor = conn.cursor()

        # Chamando a função client
        client(df_trusted, cursor)
        # Salvando controle atual para executar apenas daqui 3 proximas execuções
        salvar_controle(total_atual)
    else:
        print("Aguardando mais dados...")

# Executando main()
if __name__ == "__main__":
    main()