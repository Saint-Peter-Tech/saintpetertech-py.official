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
    for obj in page["Contents"]:
        chave = obj["Key"]
        response = s3.get_object(Bucket=bucket, Key=chave)
        registros.append({"conteudo": response})

registros = sorted(registros, key=lambda x: x["conteudo"]["LastModified"], reverse=True)

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


def ler_controle():
    if not os.path.exists(controleArquivo):
        return 0
    with open(controleArquivo, "r") as f:
        return int(f.read())


def salvar_controle(valor):
    with open(controleArquivo, "w") as f:
        f.write(str(valor))


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


def trusted(df):
    print("Processando camada TRUSTED...")
    df = preparar_raw(df)

    df_last = df.tail(10)

    if df_last.empty:
        print("Sem dados para trusted")
        return

    horarioInicio = df_last["timestamp"].min()
    horarioFim = df_last["timestamp"].max()
    idMonitor = df_last["id_monitor"].iloc[-1]

    maxCPU = df_last["cpu_percent"].max()
    maxRAM = df_last["ram_percent"].max()
    maxDISK = df_last["disk_used"].max()

    mincpuporcentagem = df_last["cpu_percent"].min()
    minramporcentagem = df_last["ram_percent"].min()
    ultimacapturacpu = df_last["cpu_percent"].iloc[-1]
    ultimacapturaram = df_last["ram_percent"].iloc[-1]

    somaRede = ((df_last["bytes_sent_per_sec"] +
                 df_last["bytes_recv_per_sec"]) * 8 / 1_000_000).round(2)

    minredeMBS = somaRede.min()
    ultimacapturarede = somaRede.iloc[-1]
    bandaLarga = somaRede.max()

    download = df_last["bytes_sent_per_sec"].iloc[-1] - df_last["bytes_sent_per_sec"].iloc[-2]
    upload = df_last["bytes_recv_per_sec"].iloc[-1] - df_last["bytes_recv_per_sec"].iloc[-2]
    trafegoTotal = download + upload

    discoUsado = float(df_last["disk_used"].iloc[-1])
    discoTotal = float(df_last["disk_total"].iloc[-1])

    kpi_rede_zero = (somaRede <= 0.01).sum()

    ultimo = df_last.iloc[-1]

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
        "kpi_rede_zero": kpi_rede_zero,
        "diskUsed": discoUsado,
        "diskTotal": discoTotal,
        "download": download,
        "upload": upload,
        "trafegoTotal": trafegoTotal
    }

    for col in status_cols:
        registro[col] = ultimo[col]

    temp_trusted = pd.DataFrame([registro])

    try:
        response = s3.get_object(Bucket=bucket, Key='trusted/trusted.csv')
        conteudo = response['Body'].read().decode('utf-8')
        df_existente = pd.read_csv(StringIO(conteudo))
        df_trusted = pd.concat([df_existente, temp_trusted], ignore_index=True)
    except s3.exceptions.NoSuchKey:
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


def client(df, cursor):
    print("Processando camada Client...\n")
    df_client = df.tail(3)

    if df_client.empty:
        print("Sem dados")
        return

    resultado = []

    for _, row in df_client.iterrows():
        id_monitor = int(row["idMonitor"])
        intervalo = round(((datetime.strptime(str(row["horarioFim"]), "%Y-%m-%d %H:%M:%S")) - (datetime.strptime(str(row["horarioInicio"]), "%Y-%m-%d %H:%M:%S"))).total_seconds() / 60, 2)
        horarioInicio = str(row["horarioInicio"])
        horarioFim = str(row["horarioFim"])

        limites = buscar_limites(cursor, id_monitor)

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
        diskUsed = float(row["diskUsed"])
        diskTotal = float(row["diskTotal"])
        upload = row["upload"]
        download = row["download"]
        trafegoTotal = row["trafegoTotal"]

        def status(valor, limite):
            if limite is None:
                return "Sem limite definido"
            if valor <= limite:
                return "OK"
            elif valor <= limite * 1.2:
                return "Alerta"
            return "Crítico"

        limite_cpu = limites.get("cpu")
        limite_ram = limites.get("ram")
        limite_disk = limites.get("disco_usado")
        limite_rede = limites.get("rede")

        statuscpu = status(cpu, limite_cpu)
        statusram = status(ram, limite_ram)
        statusdisco = status(disk, limite_disk)
        statusrede = status(rede, limite_rede)

        qtd_modulos_ativos = sum(row[col] == "Ativo" for col in status_cols)
        monitor_ativo = qtd_modulos_ativos > 0
        kpi_rede_zero = int(row["kpi_rede_zero"])

        if "Crítico" in [statuscpu, statusram, statusdisco, statusrede]:
            statusgeral = "Crítico"
        elif "Alerta" in [statuscpu, statusram, statusdisco, statusrede]:
            statusgeral = "Alerta"
        else:
            statusgeral = "OK"

        resultado.append({
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
                "status": statusrede,
                "quedasRede": kpi_rede_zero,
                "download": download,
                "upload": upload,
                "trafegoTotal": trafegoTotal
            },
            "modulos": {
                col: row[col] for col in status_cols
            }
        })

    arquivos_client = {
        "client/menegaldo.json": {},
        "client/seiti.json": {},
        "client/gustavo.json": {},
        "client/maria.json": {},
        "client/pedro.json": {
            "monitoramento": resultado
        },
        "client/philipi.json": {}
    }

    for caminho, conteudo in arquivos_client.items():
        try:
            response = s3.get_object(Bucket=bucket, Key=caminho)
            conteudo_antigo = response['Body'].read().decode('utf-8')
            json_antigo = json.loads(conteudo_antigo)
        except s3.exceptions.NoSuchKey:
            json_antigo = {"monitoramento": []}

        json_antigo["monitoramento"].extend(
            conteudo.get("monitoramento", [])
        )

        json_final = json.dumps(
            json_antigo,
            indent=4,
            ensure_ascii=False
        )

        s3.put_object(
            Bucket=bucket,
            Key=caminho,
            Body=json_final
        )

        print(f"{caminho} atualizado com sucesso")

    return resultado


def main():
    df_raw = pd.read_csv(StringIO(raw))

    if df_raw.empty:
        print("RAW vazio")
        return

    df_trusted = trusted(df_raw)

    total_atual = len(df_trusted)
    total_anterior = ler_controle()

    print(f"Controle -> atual: {total_atual} - anterior: {total_anterior}")

    if total_atual - total_anterior >= 3:
        conn = conectar()
        cursor = conn.cursor()
        client(df_trusted, cursor)
        salvar_controle(total_atual)
    else:
        print("Aguardando mais dados...")


if __name__ == "__main__":
    main()