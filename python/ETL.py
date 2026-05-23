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

controleArquivo = "./controle.txt"

buffer_trusted = StringIO()
buffer_client = StringIO()

load_dotenv()

print("Variáveis de ambiente carregadas")

status_cols = [
    "bpm_status", "pa_status", "spo2_status", "resp_status",
    "temperatura_status", "pic_status", "pvc_status",
    "ecg_status", "etco2_status"
]

bucket = os.getenv("bucket")

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
    aws_session_token=os.getenv("aws_session_token")
)

print("Conectando ao S3...")
print("Buscando arquivos RAW no S3...")

paginator = s3.get_paginator('list_objects_v2')
registros = []

for page in paginator.paginate(Bucket=bucket, Prefix="raw/"):
    for obj in page.get("Contents", []):
        chave = obj["Key"]
        response = s3.get_object(Bucket=bucket, Key=chave)
        registros.append({"conteudo": response})

registros = sorted(registros, key=lambda x: x["conteudo"]["LastModified"], reverse=True)

if not registros:
    print("Nenhum RAW encontrado")
    exit()

raw = registros[0]["conteudo"]['Body'].read().decode('utf-8')

print("RAW carregado com sucesso")


def conectar():
    print("Conectando ao MySQL...")
    try:
        conn = mysql.connector.connect(
            host=os.getenv("host"),
            user=os.getenv("user"),
            password=os.getenv("password"),
            database=os.getenv("database")
        )
        if conn.is_connected():
            print("Conectado ao SQL com Sucesso")
            return conn
    except mysql.connector.Error as e:
        print(f"Erro: {e}")


def buscar_limites(cursor, id_monitor):
    query = """
        SELECT c.nome_componente, cm.limite
        FROM componente_monitor cm
        JOIN componentes c 
            ON cm.fk_componente = c.id_componente
        WHERE cm.fk_monitor = %s
    """
    cursor.execute(query, (id_monitor,))
    resultado = cursor.fetchall()

    limites = {}
    for nome, limite in resultado:
        limites[nome.lower()] = float(limite)

    return limites

def buscar_hierarquia_monitor(cursor, id_monitor):
    query = """
        SELECT
            m.id_monitor,
            m.fk_empresa,
            u.id_unidade,
            h.id_hospital,
            e.razao_social,
            h.nome_hospital,
            u.nome_unidade
        FROM monitores m
        JOIN unidades u
            ON m.fk_unidade = u.id_unidade
        JOIN hospitais h
            ON u.fk_hospital = h.id_hospital
        JOIN empresas e
            ON m.fk_empresa = e.id_empresa
        WHERE m.id_monitor = %s
    """

    cursor.execute(query, (id_monitor,))
    resultado = cursor.fetchone()

    if not resultado:
        return None

    return {
        "id_monitor": resultado[0],
        "id_empresa": resultado[1],
        "id_unidade": resultado[2],
        "id_hospital": resultado[3],
        "empresa": resultado[4],
        "hospital": resultado[5],
        "unidade": resultado[6]
    }

def preparar_raw(df):
    df = df.copy()
    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        format="%d-%m-%Y %H_%M_%S",
        errors="coerce"
    )
    df = df.dropna(subset=["timestamp"])
    df = df.sort_values(["id_monitor", "timestamp"])
    return df

def salvar_s3(caminho, novo_dado, tipo="json"):

    if tipo == "json":

        try:
            response = s3.get_object(
                Bucket=bucket,
                Key=caminho
            )

            conteudo = response['Body'].read().decode('utf-8')

            existente = json.loads(conteudo)

            if isinstance(existente, list):
                final = existente + [novo_dado]
            else:
                final = [existente, novo_dado]

        except Exception:
            final = [novo_dado]

        json_string = json.dumps(
            final,
            indent=4,
            ensure_ascii=False
        )

        s3.put_object(
            Bucket=bucket,
            Key=caminho,
            Body=json_string
        )

    elif tipo == "csv":

        try:
            response = s3.get_object(
                Bucket=bucket,
                Key=caminho
            )

            conteudo = response['Body'].read().decode('utf-8')

            df_existente = pd.read_csv(
                StringIO(conteudo)
            )

            final = pd.concat(
                [df_existente, novo_dado],
                ignore_index=True
            )

            final = final.drop_duplicates(
            subset=["id_monitor", "timestamp"]
            )

        except Exception:
            final = novo_dado

        buffer = StringIO()

        final.to_csv(
            buffer,
            index=False
        )

        s3.put_object(
            Bucket=bucket,
            Key=caminho,
            Body=buffer.getvalue()
        )

def trusted(df):
    print("Processando camada TRUSTED...")

    df = preparar_raw(df)

    if df.empty:
        print("Sem dados para trusted")
        return pd.DataFrame()

    df["upload_mbps"] = (
        df["bytes_sent_per_sec"] * 8 / 1_000_000
    ).round(4)

    df["download_mbps"] = (
        df["bytes_recv_per_sec"] * 8 / 1_000_000
    ).round(4)

    df["banda_larga"] = (
        df["upload_mbps"] +
        df["download_mbps"]
    ).round(4)

    df = df.drop(columns=[
        "bytes_sent_per_sec",
        "bytes_recv_per_sec"
    ])

    df["disk_used"] = (
    df["disk_used"] / 1024**3
    ).round(2)

    df["disk_total"] = (
    df["disk_total"] / 1024**3
    ).round(2)

    hoje = datetime.now()

    key = (
    f"trusted/"
    f"{hoje.year}/"
    f"{hoje.month:02d}/"
    f"{hoje.day:02d}/"
    f"trusted.csv"
    )

    salvar_s3(
        caminho=key,
        novo_dado=df,
        tipo="csv"
    )

    print("Trusted atualizado com sucesso")

    return df

def client(df, cursor):
    print("Processando camada Client...\n")

    id_monitor = int(df["id_monitor"].iloc[-1])

    df_client = preparar_raw(df)

    df_client = df_client[
        df_client["id_monitor"] == id_monitor
    ]

    df_client = df_client.tail(20)

    if df_client.empty:
        print("Sem dados")
        return

    id_monitor = int(df_client["id_monitor"].iloc[-1])

    hierarquia = buscar_hierarquia_monitor(cursor, id_monitor)

    if not hierarquia:
        print("Monitor não encontrado no banco")
        return

    id_empresa = hierarquia["id_empresa"]
    id_hospital = hierarquia["id_hospital"]
    id_unidade = hierarquia["id_unidade"]
    
    horarioInicio = str(df_client["timestamp"].min())
    horarioFim = str(df_client["timestamp"].max())

    intervalo = round(
        (
            df_client["timestamp"].max() -
            df_client["timestamp"].min()
        ).total_seconds() / 60,
        2
    )

    limites = buscar_limites(cursor, id_monitor)

    cpu = df_client["cpu_percent"].max()
    mincpu = df_client["cpu_percent"].min()
    ultcpu = df_client["cpu_percent"].iloc[-1]

    ram = df_client["ram_percent"].max()
    minram = df_client["ram_percent"].min()
    ultram = df_client["ram_percent"].iloc[-1]

    disk = df_client["disk_used"].max()

    rede = df_client["banda_larga"].max()
    minrede = df_client["banda_larga"].min()
    ultrede = df_client["banda_larga"].iloc[-1]

    upload = df_client["upload_mbps"].max()
    download = df_client["download_mbps"].max()
    trafego_total = df_client["banda_larga"].sum()

    diskUsed = float(df_client["disk_used"].iloc[-1])
    diskTotal = float(df_client["disk_total"].iloc[-1])

    ultimo = df_client.iloc[-1]

    qtd_modulos_ativos = 0

    for col in status_cols:
        if ultimo[col] == "Ativo":
            qtd_modulos_ativos += 1

    monitor_ativo = qtd_modulos_ativos > 0

    kpi_rede_zero = (df_client["banda_larga"] <= 0.01).sum()

    def status(valor, limite, componente):
        if limite is None:
            return "Sem limite definido"
        
        # Quanto maior pior
        if componente in ["cpu", "ram", "disco"]:
            if valor <= limite: return "OK"
            if valor <= limite * 1.2: return "Alerta"
            return "Crítico"
        
        # Quanto menor pior, velocidade de rede rápida, está bom, quando estiver abaixo a rede fica lenta
        elif componente == "rede":
            if valor < 0.001: 
                return "Crítico" 
            
            elif valor > limite:
                return "Alerta"

            else:
                return "OK"
            
        return "OK"
    
    limite_cpu = limites.get("cpu")
    limite_ram = limites.get("ram")
    limite_disk = limites.get("disco_usado")
    limite_rede = limites.get("rede")

    statuscpu = status(cpu, limite_cpu, "cpu")
    statusram = status(ram, limite_ram, "ram")
    statusdisco = status(disk, limite_disk, "disco")
    statusrede = status(rede, limite_rede, "rede")

    if "Crítico" in [statuscpu, statusram, statusdisco, statusrede]:
        statusgeral = "Crítico"
    elif "Alerta" in [statuscpu, statusram, statusdisco, statusrede]:
        statusgeral = "Alerta"
    else:
        statusgeral = "OK"

    # EXEMPLO DE CSV:

    resultado = {
    "empresa": {
        "id": id_empresa,
        "nome": hierarquia["empresa"]
    },

    "hospital": {
        "id": id_hospital,
        "nome": hierarquia["hospital"]
    },

    "unidade": {
        "id": id_unidade,
        "nome": hierarquia["unidade"]
    },

    "monitor": {
        "id": id_monitor,
        "ativo": monitor_ativo,
        "statusGeral": statusgeral,
        "quantidadeModulosAtivos": qtd_modulos_ativos
    },

    "periodo": {
        "inicio": horarioInicio,
        "fim": horarioFim,
        "intervaloMinutos": intervalo
    },

    "cpu": {
        "picoPorcentagem": cpu,
        "minimoPorcentagem": mincpu,
        "ultimaCaptura": ultcpu,
        "status": statuscpu
    },

    "ram": {
        "picoPorcentagem": ram,
        "minimoPorcentagem": minram,
        "ultimaCaptura": ultram,
        "status": statusram
    },

    "disco": {
        "discoUsado": diskUsed,
        "discoTotal": diskTotal,
        "status": statusdisco
    },

    "rede": {
        "picoMbs": rede,
        "minimoMbs": minrede,
        "ultimaCaptura": ultrede,
        "uploadPicoMbs": upload,
        "downloadPicoMbs": download,
        "trafegoTotalMbs": trafego_total,
        "status": statusrede,
        "quedasRede": int(kpi_rede_zero)
    },

    "modulos": {
        col: ultimo[col] for col in status_cols
    }
}
    
    # Criando caminhos com base na função de Hierarquia:

    base_path = (
        f"client/"
        f"empresa_{id_empresa}/"
    )

    hospital_path = (
        f"{base_path}"
        f"hospital_{id_hospital}/"
    )

    unidade_path = (
        f"{hospital_path}"
        f"unidade_{id_unidade}/"
    )

    caminho_monitor = (
        f"{unidade_path}"
        f"monitor_{id_monitor}.json"
    )

    # Dash Philipi:

    controle_json = {
        "empresa": id_empresa,
        "ultimaAtualizacao": str(datetime.now()),
        "monitor": id_monitor
    }

    salvar_s3(
        f"{base_path}controle.json",
        controle_json,
        tipo="json"
    )

    # Dash Diego Seiti:

    modelos_json = {
        "monitor": id_monitor
    }

    salvar_s3(
        f"{base_path}modelos.json",
        modelos_json,
        tipo="json"
    )

    # Dash Pedro:

    modulos_json = {
        "monitor": id_monitor,
        "modulos": {
            col: ultimo[col]
            for col in status_cols
        }
    }

    salvar_s3(
        f"{base_path}modulos.json",
        modulos_json,
        tipo="json"
    )

    # ======= DASHBOARD DIEGO HENRIQUE ======== 

    caminhoJsonHospital = f"{hospital_path}hospital.json"
    dataAtual = datetime.now()
    semanaAtual = dataAtual.isocalendar()[1] # Pega o número da semana do ano

    # Contagem de alertas na última semana
    ultimaSemana = dataAtual - timedelta(days=7) 
    df_semana = df_client[df_client["timestamp"] >= ultimaSemana].copy() 

    alertasCpu = int((df_semana["cpu_percent"].apply(lambda x: status(x, limite_cpu, "cpu")) == "Alerta").sum())
    alertasRam = int((df_semana["ram_percent"].apply(lambda x: status(x, limite_ram, "ram")) == "Alerta").sum())
    alertasDisco = int((df_semana["disk_used"].apply(lambda x: status(x, limite_disk, "disco")) == "Alerta").sum())
    alertasRede = int((df_semana["banda_larga"].apply(lambda x: status(x, limite_rede, "rede")) == "Alerta").sum())
    
    
    # A função lambda é para: a cada valor capturado, se for diferente do status OK, gere um alerta

    criticosCPU = (df_semana["cpu_percent"].apply(lambda x: status(x, limite_cpu, "cpu")) == "Crítico").sum()
    criticosRAM = (df_semana["ram_percent"].apply(lambda x: status(x, limite_ram, "ram")) == "Crítico").sum()
    critiscosDisco = (df_semana["disk_used"].apply(lambda x: status(x, limite_disk, "disco")) == "Crítico").sum()
    criticosRede = (df_semana["banda_larga"].apply(lambda x: status(x, limite_rede, "rede")) == "Crítico").sum()
    criticos = int(
        criticosCPU + criticosRAM + critiscosDisco + criticosRede
    )

    # Agora para os críticos, para cada valor capturado, se for acima de 20% do limite se categoriza como crítico

    # Filtragem se o arquivo hospital.json no diretório hospital_id já existe
    arquivoExiste = s3.list_objects_v2(Bucket=bucket, Prefix=caminhoJsonHospital)
    
    if "Contents" in arquivoExiste: # Se retornar "Contents" ele já existe
        respostaS3 = s3.get_object(Bucket=bucket, Key=caminhoJsonHospital)
        jsonHospital = json.loads(respostaS3['Body'].read().decode('utf-8'))
        
        ultimaAtualizacao = datetime.strptime(jsonHospital["ultimaAtualizacao"], "%Y-%m-%d %H:%M:%S")
        semanaPassada = ultimaAtualizacao.isocalendar()[1]
        
    else: # Criação do arquivo caso não exista
        jsonHospital = {
            "id": id_hospital,
            "nome": hierarquia["hospital"],
            "ultimaAtualizacao": dataAtual.strftime("%Y-%m-%d %H:%M:%S"),
            "alertasSemanais": {
                "totalAlertas": 0,
                    "porComponente": {"cpu": 0, "ram": 0, "disco": 0, "rede": 0}
            },
            "criticos": {
                "totalCriticos": 0,
                "porComponente": {"cpuCritico": 0, "ramCritico": 0, "discoCritico": 0, "redeCritico": 0}
            },
            "alertasSemanaPassada": {
                "totalAlertas": 0,
                "totalCriticos" : 0
            }
        }
        
        semanaPassada = semanaAtual

    if semanaAtual > semanaPassada: # Valida se a semana virou e atribui os valores da semana atual na passada e reseta a atual
        jsonHospital["alertasSemanaPassada"]["totalAlertas"] = jsonHospital["alertasSemanais"]["totalAlertas"]
        jsonHospital["alertasSemanaPassada"]["totalCriticos"] = jsonHospital["criticos"]["totalCriticos"]      

        jsonHospital["alertasSemanais"]["totalAlertas"] = 0
        jsonHospital["criticos"]["totalCriticos"] = 0
        jsonHospital["alertasSemanais"]["porComponente"] = {"cpu": 0, "ram": 0, "disco": 0, "rede": 0}
        jsonHospital["criticos"]["porComponente"] = {"cpuCritico": 0, "ramCritico": 0, "discoCritico" : 0, "redeCritico" : 0}
        

    # Acumula o número de alertas
    jsonHospital["ultimaAtualizacao"] = dataAtual.strftime("%Y-%m-%d %H:%M:%S")
    jsonHospital["alertasSemanais"]["porComponente"]["cpu"] += int(alertasCpu)
    jsonHospital["alertasSemanais"]["porComponente"]["ram"] += int(alertasRam)
    jsonHospital["alertasSemanais"]["porComponente"]["disco"] += int(alertasDisco)
    jsonHospital["alertasSemanais"]["porComponente"]["rede"] += int(alertasRede)
    jsonHospital["criticos"]["totalCriticos"] += int(criticos)
    jsonHospital["criticos"]["porComponente"]["cpuCritico"] += int(criticosCPU)
    jsonHospital["criticos"]["porComponente"]["ramCritico"] += int(criticosRAM)
    jsonHospital["criticos"]["porComponente"]["discoCritico"] += int(critiscosDisco)
    jsonHospital["criticos"]["porComponente"]["redeCritico"] += int(criticosRede)

    # Calcula novamente os alertas semanais para acumular)
    jsonHospital["alertasSemanais"]["totalAlertas"] = sum(jsonHospital["alertasSemanais"]["porComponente"].values())
    jsonHospital["criticos"]["totalCriticos"] = sum(jsonHospital["criticos"]["porComponente"].values())

    s3.put_object(
        Bucket=bucket,
        Key=caminhoJsonHospital,
        Body=json.dumps(jsonHospital, ensure_ascii=False), # ensure ascii garante que se houver acentos eles não serão substituídos
        ContentType='application/json'
    )
    # ========== FIM DA DASH DO DIEGO HENRIQUE ==========

    # Dash Gustavo:

    unidade_json = {
        "id": id_unidade,
        "nome": hierarquia["unidade"]
    }

    salvar_s3(
        f"{unidade_path}unidade.json",
        unidade_json,
        tipo="json"
    )

    # Dash Maria:

    monitor_json = {
        "id": id_monitor,
        "ativo": monitor_ativo,
        "statusGeral": statusgeral,
        "quantidadeModulosAtivos": qtd_modulos_ativos
    }

    salvar_s3(
        caminho_monitor,
        monitor_json,
        tipo="json"
    )

    print("CLIENT atualizado")
    print(f"Empresa: {id_empresa}")
    print(f"Hospital: {id_hospital}")
    print(f"Unidade: {id_unidade}")
    print(f"Monitor: {id_monitor}")

    return resultado


def main():
    df_raw = pd.read_csv(StringIO(raw))

    if df_raw.empty:
        print("RAW vazio")
        return

    df_trusted = trusted(df_raw)

    conn = conectar()
    cursor = conn.cursor()
    client(df_trusted, cursor)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()