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
    "bpm_status",
    "pa_status",
    "spo2_status",
    "resp_status",
    "temperatura_status",
    "pic_status",
    "pvc_status",
    "ecg_status",
    "etco2_status",
]

bucket = os.getenv("bucket")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("aws_access_key_id"),
    aws_secret_access_key=os.getenv("aws_secret_access_key"),
    aws_session_token=os.getenv("aws_session_token"),
)

print("Conectando ao S3...")
print("Buscando arquivos RAW no S3...")

paginator = s3.get_paginator("list_objects_v2")
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

raw = registros[0]["conteudo"]["Body"].read().decode("utf-8")

print("RAW carregado com sucesso")


def conectar():
    print("Conectando ao MySQL...")
    try:
        conn = mysql.connector.connect(
            host=os.getenv("host"),
            user=os.getenv("user"),
            password=os.getenv("password"),
            database=os.getenv("database"),
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
            u.nome_unidade,
            mo.id_modelo,
            mo.nome
        FROM monitores m
        JOIN unidades u
            ON m.fk_unidade = u.id_unidade
        JOIN hospitais h
            ON u.fk_hospital = h.id_hospital
        JOIN empresas e
            ON m.fk_empresa = e.id_empresa
        JOIN modelos mo
            ON m.fk_modelo = mo.id_modelo
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
        "unidade": resultado[6],
        "id_modelo": resultado[7],
        "modelo": resultado[8],
    }

def buscar_rede_total_unidade(cursor, id_unidade):
    query = """
        SELECT rede_total
        FROM unidades
        WHERE id_unidade = %s
    """
    cursor.execute(query, (id_unidade,))
    resultado = cursor.fetchone()
    if resultado:
        return float(resultado[0])
    return None

def buscar_monitores_modelo(cursor, id_modelo):

    query = """
        SELECT id_monitor
        FROM monitores
        WHERE fk_modelo = %s
    """

    cursor.execute(query, (id_modelo,))

    resultado = cursor.fetchall()

    return [x[0] for x in resultado]


def preparar_raw(df):
    df = df.copy()
    df["timestamp"] = pd.to_datetime(
        df["timestamp"], format="%d-%m-%Y %H_%M_%S", errors="coerce"
    )
    df = df.dropna(subset=["timestamp"])
    df = df.sort_values(["id_monitor", "timestamp"])
    return df


def salvar_s3(caminho, novo_dado, tipo="json"):

    if tipo == "json":

        try:
            response = s3.get_object(Bucket=bucket, Key=caminho)

            conteudo = response["Body"].read().decode("utf-8")

            existente = json.loads(conteudo)

            if isinstance(existente, list):
                final = existente + [novo_dado]
            else:
                final = [existente, novo_dado]

        except Exception:
            final = [novo_dado]

        json_string = json.dumps(final, indent=4, ensure_ascii=False)

        s3.put_object(Bucket=bucket, Key=caminho, Body=json_string)

    elif tipo == "csv":

        try:
            response = s3.get_object(Bucket=bucket, Key=caminho)

            conteudo = response["Body"].read().decode("utf-8")

            df_existente = pd.read_csv(StringIO(conteudo))

            final = pd.concat([df_existente, novo_dado], ignore_index=True)

            final = final.drop_duplicates(subset=["id_monitor", "timestamp"])

        except Exception:
            final = novo_dado

        buffer = StringIO()

        final.to_csv(buffer, index=False)

        s3.put_object(Bucket=bucket, Key=caminho, Body=buffer.getvalue())


def trusted(df):
    print("Processando camada TRUSTED...")

    df = preparar_raw(df)

    if df.empty:
        print("Sem dados para trusted")
        return pd.DataFrame()

    df["upload_mbps"] = (df["bytes_sent_per_sec"] * 8 / 1_000_000).round(4)

    df["download_mbps"] = (df["bytes_recv_per_sec"] * 8 / 1_000_000).round(4)

    df["banda_larga"] = (df["upload_mbps"] + df["download_mbps"]).round(4)

    df = df.drop(columns=["bytes_sent_per_sec", "bytes_recv_per_sec"])

    df["disk_used"] = (df["disk_used"] / 1024**3).round(2)

    df["disk_total"] = (df["disk_total"] / 1024**3).round(2)

    df["disk_percent"] = ((df["disk_used"] / df["disk_total"]) * 100).round(2)

    hoje = datetime.now()

    key = (
        f"trusted/"
        f"{hoje.year}/"
        f"{hoje.month:02d}/"
        f"{hoje.day:02d}/"
        f"trusted.csv"
    )

    salvar_s3(caminho=key, novo_dado=df, tipo="csv")

    print("Trusted atualizado com sucesso")

    return df


def client(df, cursor):
    print("Processando camada Client...\n")

    id_monitor = int(df["id_monitor"].iloc[-1])

    df_client = preparar_raw(df)

    df_client = df_client[df_client["id_monitor"] == id_monitor]

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

    rede_total_unidade = buscar_rede_total_unidade(cursor, id_unidade)

    horarioInicio = str(df_client["timestamp"].min())
    horarioFim = str(df_client["timestamp"].max())

    intervalo = round(
        (df_client["timestamp"].max() - df_client["timestamp"].min()).total_seconds()
        / 60,
        2,
    )

    limites = buscar_limites(cursor, id_monitor)

    cpu = df_client["cpu_percent"].max()
    mincpu = df_client["cpu_percent"].min()
    ultcpu = df_client["cpu_percent"].iloc[-1]

    ram = df_client["ram_percent"].max()
    minram = df_client["ram_percent"].min()
    ultram = df_client["ram_percent"].iloc[-1]

    disk = df_client["disk_used"].max()
    disk_percent = df_client["disk_percent"].max()

    rede = df_client["banda_larga"].max()
    minrede = df_client["banda_larga"].min()
    ultrede = df_client["banda_larga"].iloc[-1]
    redeAtual = df_client["banda_larga"].iloc[-1]

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
            if valor <= limite:
                return "OK"
            if valor <= limite * 1.2:
                return "Alerta"
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
    statusdisco = status(disk_percent, limite_disk, "disco")
    statusrede = status(rede, limite_rede, "rede")

    if "Crítico" in [statuscpu, statusram, statusdisco, statusrede]:
        statusgeral = "Crítico"
    elif "Alerta" in [statuscpu, statusram, statusdisco, statusrede]:
        statusgeral = "Alerta"
    else:
        statusgeral = "OK"

    # EXEMPLO DE CSV:

    resultado = {
        "empresa": {"id": id_empresa, "nome": hierarquia["empresa"]},
        "hospital": {"id": id_hospital, "nome": hierarquia["hospital"]},
        "unidade": {"id": id_unidade, "nome": hierarquia["unidade"]},
        "monitor": {
            "id": id_monitor,
            "ativo": monitor_ativo,
            "statusGeral": statusgeral,
            "quantidadeModulosAtivos": qtd_modulos_ativos,
        },
        "periodo": {
            "inicio": horarioInicio,
            "fim": horarioFim,
            "intervaloMinutos": intervalo,
        },
        "cpu": {
            "picoPorcentagem": cpu,
            "minimoPorcentagem": mincpu,
            "ultimaCaptura": ultcpu,
            "status": statuscpu,
        },
        "ram": {
            "picoPorcentagem": ram,
            "minimoPorcentagem": minram,
            "ultimaCaptura": ultram,
            "status": statusram,
        },
        "disco": {
            "discoUsado": diskUsed,
            "discoTotal": diskTotal,
            "status": statusdisco,
        },
        "rede": {
            "picoMbs": rede,
            "minimoMbs": minrede,
            "ultimaCaptura": ultrede,
            "uploadPicoMbs": upload,
            "downloadPicoMbs": download,
            "trafegoTotalMbs": trafego_total,
            "status": statusrede,
            "quedasRede": int(kpi_rede_zero),
        },
        "modulos": {col: ultimo[col] for col in status_cols},
    }

    # Criando caminhos com base na função de Hierarquia:

    base_path = f"client/" f"empresa_{id_empresa}/"

    hospital_path = f"{base_path}" f"hospital_{id_hospital}/"

    unidade_path = f"{hospital_path}" f"unidade_{id_unidade}/"

    caminho_monitor = f"{unidade_path}" f"monitor_{id_monitor}.json"

    # Dash Philipi:

    controle_json = {
        "empresa": id_empresa,
        "ultimaAtualizacao": str(datetime.now()),
        "monitor": id_monitor,
    }

    salvar_s3(f"{base_path}controle.json", controle_json, tipo="json")

   # ======= DASHBOARD DIEGO SEITI ========

    caminhoJsonModelos = (f"client/" f"empresa_{hierarquia['id_empresa']}/" f"modelos/modelos.json")

    capturasTotais = len(df_client)

    alertasCpu = int(
        (
            df_client["cpu_percent"].apply(lambda x: status(x, limite_cpu, "cpu"))
            == "Alerta"
        ).sum()
    )
    alertasRam = int(
        (
            df_client["ram_percent"].apply(lambda x: status(x, limite_ram, "ram"))
            == "Alerta"
        ).sum()
    )
    alertasRede = int(
        (
            df_client["banda_larga"].apply(lambda x: status(x, limite_rede, "rede"))
            == "Alerta"
        ).sum()
    )

    criticosCPU = (
        df_client["cpu_percent"].apply(lambda x: status(x, limite_cpu, "cpu"))
        == "Crítico"
    ).sum()
    criticosRAM = (
        df_client["ram_percent"].apply(lambda x: status(x, limite_ram, "ram"))
        == "Crítico"
    ).sum()
    criticosRede = (
        df_client["banda_larga"].apply(lambda x: status(x, limite_rede, "rede"))
        == "Crítico"
    ).sum()

    status_cpu = df_client["cpu_percent"].apply(
        lambda x: status(x, limite_cpu, "cpu")
    )

    status_ram = df_client["ram_percent"].apply(
        lambda x: status(x, limite_ram, "ram")
    )

    status_rede = df_client["banda_larga"].apply(
        lambda x: status(x, limite_rede, "rede")
    )

    linhas_com_alerta = (
        (status_cpu != "OK")
        | (status_ram != "OK")
        | (status_rede != "OK")
    )

    qtdCapturasAlerta = int(linhas_com_alerta.sum())

    try:

        response = s3.get_object(
            Bucket=bucket,
            Key=caminhoJsonModelos
        )

        conteudo = response["Body"].read().decode("utf-8")

        jsonModelos = json.loads(conteudo)

    except Exception:

        jsonModelos = {
            "modelos": {}
        }

    idModelo = str(hierarquia["id_modelo"])

    if idModelo not in jsonModelos["modelos"]:

        jsonModelos["modelos"][idModelo] = {

            "id_modelo": hierarquia["id_modelo"],
            "nome": hierarquia["modelo"],

            "capturas_totais": 0,
            "capturas_alertas": 0,
            "pct_alertas": 0,

            "alertas_cpu": 0,
            "criticos_cpu": 0,
            "pct_alertas_cpu": 0,
            "pct_criticos_cpu": 0,
            "pct_ok_cpu": 0,
            "origem_alertas_cpu": 0,
            "healthscore_cpu": 100,

            "alertas_ram": 0,
            "criticos_ram": 0,
            "pct_alertas_ram": 0,
            "pct_criticos_ram": 0,
            "pct_ok_ram": 0,
            "origem_alertas_ram": 0,
            "healthscore_cpu": 100,

            "alertas_rede": 0,
            "criticos_rede": 0,
            "pct_alertas_rede": 0,
            "pct_criticos_rede": 0,
            "pct_ok_rede": 0,
            "origem_alertas_rede": 0,
            "healthscore_cpu": 100,

            "cpu_acima_limite": 0,
            "ram_acima_limite": 0,
            "rede_acima_limite": 0,

            "healthscore": 100
        }

    modelo = jsonModelos["modelos"][idModelo]

    modelo["capturas_totais"] += capturasTotais
    modelo["capturas_alertas"] += qtdCapturasAlerta
    modelo["pct_alertas"] = round(
        modelo["capturas_alertas"] * 100 / modelo["capturas_totais"],
        2
    )

    modelo["alertas_cpu"] += int(alertasCpu)
    modelo["criticos_cpu"] += int(criticosCPU)

    modelo["alertas_ram"] += int(alertasRam)
    modelo["criticos_ram"] += int(criticosRAM)

    modelo["alertas_rede"] += int(alertasRede)
    modelo["criticos_rede"] += int(criticosRede)

    totalCpu = (
        modelo["alertas_cpu"]
        + modelo["criticos_cpu"]
    )

    totalRam = (
        modelo["alertas_ram"]
        + modelo["criticos_ram"]
    )

    totalRede = (
        modelo["alertas_rede"]
        + modelo["criticos_rede"]
    )

    totalCapturas = modelo["capturas_totais"]

    modelo["cpu_acima_limite"] = round(
        (totalCpu * 100) / totalCapturas,
        2
    )

    modelo["ram_acima_limite"] = round(
        (totalRam * 100) / totalCapturas,
        2
    )

    modelo["rede_acima_limite"] = round(
        (totalRede * 100) / totalCapturas,
        2
    )

    pctAlertaCpu = (
        modelo["alertas_cpu"] * 100
    ) / totalCapturas

    pctCriticoCpu = (
        modelo["criticos_cpu"] * 100
    ) / totalCapturas

    pctAlertaRam = (
        modelo["alertas_ram"] * 100
    ) / totalCapturas

    pctCriticoRam = (
        modelo["criticos_ram"] * 100
    ) / totalCapturas

    pctAlertaRede = (
        modelo["alertas_rede"] * 100
    ) / totalCapturas

    pctCriticoRede = (
        modelo["criticos_rede"] * 100
    ) / totalCapturas

    healthScoreCpu = (100 - (pctAlertaCpu + 2 * pctCriticoCpu))
    healthScoreRam = (100 - (pctAlertaRam + 2 * pctCriticoRam))
    healthScoreRede = (100 - (pctAlertaRede + 2 * pctCriticoRede))

    modelo["healthscore_cpu"] = round(max(0, min(100, healthScoreCpu)), 2)
    modelo["healthscore_ram"] = round(max(0, min(100, healthScoreRam)), 2)
    modelo["healthscore_rede"] = round(max(0, min(100, healthScoreRede)), 2)

    healthScore = (
        + (modelo["healthscore_cpu"] * 0.25)
        + (modelo["healthscore_ram"] * 0.25)
        + (modelo["healthscore_rede"] * 0.5)
    )

    modelo["healthscore"] = round(
        max(0, min(100, healthScore)),
        2
    )

    modelo["pct_alertas_cpu"] = round(pctAlertaCpu, 1)
    modelo["pct_criticos_cpu"] = round(pctCriticoCpu, 1)
    modelo["pct_ok_cpu"] = 100 - (modelo["pct_alertas_cpu"] + modelo["pct_criticos_cpu"])
    modelo["pct_alertas_ram"] = round(pctAlertaRam, 1)
    modelo["pct_criticos_ram"] = round(pctCriticoRam, 1)
    modelo["pct_ok_ram"] = 100 - (modelo["pct_alertas_ram"] + modelo["pct_criticos_ram"])
    modelo["pct_alertas_rede"] = round(pctAlertaRede, 1)
    modelo["pct_criticos_rede"] = round(pctCriticoRede, 1)
    modelo["pct_ok_rede"] = 100 - (modelo["pct_alertas_rede"] + modelo["pct_criticos_rede"])

    alertasTotais = totalCpu + totalRam + totalRede

    modelo["origem_alertas_cpu"] = round(
        totalCpu * 100 / alertasTotais, 2
    )
    modelo["origem_alertas_ram"] = round(
        totalRam * 100 / alertasTotais, 2
    )
    modelo["origem_alertas_rede"] = round(
        totalRede * 100 / alertasTotais, 2
    )

    s3.put_object(
        Bucket=bucket,
        Key=caminhoJsonModelos,
        Body=json.dumps(
            jsonModelos, 
            ensure_ascii=False
        ),
        ContentType="application/json",
    )

    # ======= FIM DA DASHBOARD DIEGO SEITI ========

    # ======= DASHBOARD PEDRO SOUSA ========

    id_modelo = hierarquia["id_modelo"]

    nome_modelo = hierarquia["modelo"]

    monitores_modelo = buscar_monitores_modelo(cursor, id_modelo)

    df_modelo = preparar_raw(df)

    df_modelo = df_modelo[df_modelo["id_monitor"].isin(monitores_modelo)]

    if df_modelo.empty:
        print("Sem dados para o modelo")
        return

    primeiraCaptura = pd.to_datetime(df_modelo["timestamp"].min())

    ultimaCaptura = pd.to_datetime(df_modelo["timestamp"].max())

    diasCaptura = max((ultimaCaptura - primeiraCaptura).days, 1)

    modulos_map = {
        "BPM": "bpm_status",
        "PA": "pa_status",
        "SPO2": "spo2_status",
        "RESP": "resp_status",
        "TEMP": "temperatura_status",
        "PIC": "pic_status",
        "PVC": "pvc_status",
        "ECG": "ecg_status",
        "ETCO2": "etco2_status",
    }

    modulos_analise = {}

    for nome in modulos_map.keys():

        modulos_analise[nome] = {
            "ativos": 0,
            "inativos": 0,
            "ok": 0,
            "alerta": 0,
            "critico": 0,
            "usoPercentual": 0,
        }

    totalCapturas = len(df_modelo)

    for _, row in df_modelo.iterrows():

        statuscpu = status(row["cpu_percent"], limite_cpu, "cpu")

        statusram = status(row["ram_percent"], limite_ram, "ram")

        statusdisco = status(row["disk_percent"], limite_disk, "disco")

        statusrede = status(row["banda_larga"], limite_rede, "rede")

        statuses = [statuscpu, statusram, statusdisco, statusrede]

        if "Crítico" in statuses:
            statusgeral = "Crítico"

        elif "Alerta" in statuses:
            statusgeral = "Alerta"

        else:
            statusgeral = "OK"

        for nome, coluna in modulos_map.items():

            statusModulo = row[coluna]

            if statusModulo == "Ativo":

                modulos_analise[nome]["ativos"] += 1

                if statusgeral == "OK":
                    modulos_analise[nome]["ok"] += 1

                elif statusgeral == "Alerta":
                    modulos_analise[nome]["alerta"] += 1

                elif statusgeral == "Crítico":
                    modulos_analise[nome]["critico"] += 1

            else:

                modulos_analise[nome]["inativos"] += 1

    moduloMaisUtilizado = max(modulos_analise.items(), key=lambda x: x[1]["ativos"])

    moduloMaisCritico = max(
        modulos_analise.items(), key=lambda x: (x[1]["alerta"] + x[1]["critico"])
    )

    capturasProblema = 0

    for _, row in df_modelo.iterrows():

        statuses = [
            status(row["cpu_percent"], limite_cpu, "cpu"),
            status(row["ram_percent"], limite_ram, "ram"),
            status(row["disk_percent"], limite_disk, "disco"),
            status(row["banda_larga"], limite_rede, "rede"),
        ]

        if "Alerta" in statuses or "Crítico" in statuses:
            capturasProblema += 1

    instabilidadeGeral = (
        round((capturasProblema / totalCapturas) * 100, 2) if totalCapturas > 0 else 0
    )

    estabilidadeGeral = round(100 - instabilidadeGeral, 2)

    meta = 80

    metaAtual = max(round(meta - estabilidadeGeral, 2), 0)

    modulosLista = []

    for nome, dados in modulos_analise.items():

        totalModulo = dados["ativos"] + dados["inativos"]

        problemas = dados["alerta"] + dados["critico"]

        instabilidade = (
            round((problemas / totalModulo) * 100, 2) if totalModulo > 0 else 0
        )

        estabilidade = round(100 - instabilidade, 2)

        modulosLista.append(
            {
                "nome": nome,
                "ativos": dados["ativos"],
                "inativos": dados["inativos"],
                "ok": dados["ok"],
                "alerta": dados["alerta"],
                "critico": dados["critico"],
                "usoPercentual": 0,
                "instabilidade": instabilidade,
                "estabilidade": estabilidade,
            }
        )

    caminhoModelo = f"{base_path}" f"modelos/" f"modelo_{id_modelo}.json"

    try:

        response = s3.get_object(Bucket=bucket, Key=caminhoModelo)

        json_antigo = json.loads(response["Body"].read().decode("utf-8"))

    except Exception:

        json_antigo = None

    if json_antigo:

        modulos_antigos = {m["nome"]: m for m in json_antigo["modulos"]}

        primeiraAntiga = pd.to_datetime(json_antigo["periodo"]["inicio"])

        ultimaAntiga = pd.to_datetime(json_antigo["periodo"]["fim"])

        primeiraCaptura = min(primeiraCaptura, primeiraAntiga)

        ultimaCaptura = max(ultimaCaptura, ultimaAntiga)

        diasCaptura = max((ultimaCaptura - primeiraCaptura).days, 1)

        for modulo in modulosLista:

            nome = modulo["nome"]

            if nome in modulos_antigos:

                antigo = modulos_antigos[nome]

                modulo["ativos"] += antigo["ativos"]
                modulo["inativos"] += antigo["inativos"]

                modulo["ok"] += antigo["ok"]
                modulo["alerta"] += antigo["alerta"]
                modulo["critico"] += antigo["critico"]

    for modulo in modulosLista:

        ativos = modulo["ativos"]
        inativos = modulo["inativos"]

        totalModulo = ativos + inativos

        alertas = modulo["alerta"]

        modulo["usoPercentual"] = (
            round((ativos / totalModulo) * 100, 2) if totalModulo > 0 else 0
        )

    moduloMaisUtilizado = max(modulosLista, key=lambda x: x["ativos"])

    moduloMaisCritico = max(modulosLista, key=lambda x: (x["alerta"] + x["critico"]))

    totalAlertas = sum(modulo["alerta"] + modulo["critico"] for modulo in modulosLista)

    totalCapturasSistema = len(df_modelo)

    estabilidadeGeral = max(
        round(100 - ((totalAlertas / totalCapturasSistema) * 100), 2), 0
    )

    meta = 80

    metaAtual = max(round(meta - estabilidadeGeral, 2), 0)

    modulos_json = {
        "modelo": {"id": id_modelo, "nome": nome_modelo},
        "ultimaAtualizacao": str(datetime.now()),
        "periodo": {
            "diasCaptura": diasCaptura,
            "inicio": str(primeiraCaptura),
            "fim": str(ultimaCaptura),
        },
        "kpis": {
            "moduloMaisUtilizado": moduloMaisUtilizado["nome"],
            "frequenciaMaisUtilizada": moduloMaisUtilizado["usoPercentual"],
            "moduloMaisCritico": moduloMaisCritico["nome"],
            "ocorrenciasCriticas": (
                moduloMaisCritico["alerta"] + moduloMaisCritico["critico"]
            ),
            "estabilidadeGeral": estabilidadeGeral,
            "metaAtual": metaAtual,
            "meta": meta,
        },
        "modulos": modulosLista,
    }

    caminhoModelo = f"{base_path}" f"modelos/" f"modelo_{id_modelo}.json"

    s3.put_object(
        Bucket=bucket,
        Key=caminhoModelo,
        Body=json.dumps(modulos_json, ensure_ascii=False, indent=4),
        ContentType="application/json",
    )

    # ========== FIM DA DASH DO PEDRO SOUSA ==========

    # ======= DASHBOARD DIEGO HENRIQUE ========

    caminhoJsonHospital = f"{hospital_path}hospital.json"
    dataAtual = datetime.now()
    semanaAtual = dataAtual.isocalendar()[1]  # Pega o número da semana do ano

    # Contagem de alertas na última semana
    ultimaSemana = dataAtual - timedelta(days=7)
    df_semana = df_client[df_client["timestamp"] >= ultimaSemana].copy()

    alertasCpu = int(
        (
            df_semana["cpu_percent"].apply(lambda x: status(x, limite_cpu, "cpu"))
            == "Alerta"
        ).sum()
    )
    alertasRam = int(
        (
            df_semana["ram_percent"].apply(lambda x: status(x, limite_ram, "ram"))
            == "Alerta"
        ).sum()
    )
    alertasDisco = int(
        (
            df_semana["disk_percent"].apply(lambda x: status(x, limite_disk, "disco"))
            == "Alerta"
        ).sum()
    )
    alertasRede = int(
        (
            df_semana["banda_larga"].apply(lambda x: status(x, limite_rede, "rede"))
            == "Alerta"
        ).sum()
    )

    # A função lambda é para: a cada x valor capturado, se for do status Alerta, gere um alerta comum

    criticosCPU = (
        df_semana["cpu_percent"].apply(lambda x: status(x, limite_cpu, "cpu"))
        == "Crítico"
    ).sum()
    criticosRAM = (
        df_semana["ram_percent"].apply(lambda x: status(x, limite_ram, "ram"))
        == "Crítico"
    ).sum()
    criticosDisco = (
        df_semana["disk_percent"].apply(lambda x: status(x, limite_disk, "disco"))
        == "Crítico"
    ).sum()
    criticosRede = (
        df_semana["banda_larga"].apply(lambda x: status(x, limite_rede, "rede"))
        == "Crítico"
    ).sum()
    criticos = int(criticosCPU + criticosRAM + criticosDisco + criticosRede)

    # Agora para os críticos, para cada valor capturado, se for acima de 20% do limite se categoriza como crítico

    # Filtragem se o arquivo hospital.json no diretório hospital_id já existe
    arquivoExiste = s3.list_objects_v2(Bucket=bucket, Prefix=caminhoJsonHospital)

    if "Contents" in arquivoExiste:  # Se retornar "Contents" ele já existe
        respostaS3 = s3.get_object(Bucket=bucket, Key=caminhoJsonHospital)
        jsonHospital = json.loads(respostaS3["Body"].read().decode("utf-8"))

        ultimaAtualizacao = datetime.strptime(
            jsonHospital["ultimaAtualizacao"], "%Y-%m-%d %H:%M:%S"
        )
        semanaPassada = ultimaAtualizacao.isocalendar()[1]

    else:  # Criação do arquivo caso não exista
        jsonHospital = {
            "id": id_hospital,
            "nome": hierarquia["hospital"],
            "ultimaAtualizacao": dataAtual.strftime("%Y-%m-%d %H:%M:%S"),
            "alertasSemanais": {
                "totalAlertas": 0,
                "porComponente": {"cpu": 0, "ram": 0, "disco": 0, "rede": 0},
            },
            "criticos": {
                "totalCriticos": 0,
                "porComponente": {
                    "cpuCritico": 0,
                    "ramCritico": 0,
                    "discoCritico": 0,
                    "redeCritico": 0,
                },
            },
            "unidades": {},
            "unidadeMaisCritica": {"nome": "Não encontrado", "totalCriticos": 0},
            "alertasSemanaPassada": {
                "totalAlertas": 0,
                "totalCriticos": 0,
                "porComponente": {"cpu": 0, "ram": 0, "disco": 0, "rede": 0},
                "criticosPorComponente": {
                    "cpuCritico": 0,
                    "ramCritico": 0,
                    "discoCritico": 0,
                    "redeCritico": 0,
                },
            },
        }

        semanaPassada = semanaAtual

    if semanaAtual > semanaPassada:
        jsonHospital["alertasSemanaPassada"]["totalAlertas"] = jsonHospital[
            "alertasSemanais"
        ]["totalAlertas"]
        jsonHospital["alertasSemanaPassada"]["totalCriticos"] = jsonHospital[
            "criticos"
        ]["totalCriticos"]

        jsonHospital["alertasSemanaPassada"]["porComponente"] = dict(
            jsonHospital["alertasSemanais"]["porComponente"]
        )
        jsonHospital["alertasSemanaPassada"]["criticosPorComponente"] = dict(
            jsonHospital["criticos"]["porComponente"]
        )

        jsonHospital["alertasSemanais"]["totalAlertas"] = 0
        jsonHospital["criticos"]["totalCriticos"] = 0
        jsonHospital["alertasSemanais"]["porComponente"] = {
            "cpu": 0,
            "ram": 0,
            "disco": 0,
            "rede": 0,
        }
        jsonHospital["criticos"]["porComponente"] = {
            "cpuCritico": 0,
            "ramCritico": 0,
            "discoCritico": 0,
            "redeCritico": 0,
        }
        jsonHospital["unidades"] = {}
        jsonHospital["unidadeMaisCritica"] = {
            "nome": "Não encontrado",
            "totalCriticos": 0,
        }

    # Acumula o número de alertas
    jsonHospital["ultimaAtualizacao"] = dataAtual.strftime("%Y-%m-%d %H:%M:%S")
    jsonHospital["alertasSemanais"]["porComponente"]["cpu"] += int(alertasCpu)
    jsonHospital["alertasSemanais"]["porComponente"]["ram"] += int(alertasRam)
    jsonHospital["alertasSemanais"]["porComponente"]["disco"] += int(alertasDisco)
    jsonHospital["alertasSemanais"]["porComponente"]["rede"] += int(alertasRede)
    jsonHospital["criticos"]["totalCriticos"] += int(criticos)
    jsonHospital["criticos"]["porComponente"]["cpuCritico"] += int(criticosCPU)
    jsonHospital["criticos"]["porComponente"]["ramCritico"] += int(criticosRAM)
    jsonHospital["criticos"]["porComponente"]["discoCritico"] += int(criticosDisco)
    jsonHospital["criticos"]["porComponente"]["redeCritico"] += int(criticosRede)

    # Classificação de criticidade por unidade
    unidadeKey = str(id_unidade)
    if unidadeKey not in jsonHospital["unidades"]:
        jsonHospital["unidades"][unidadeKey] = {
            "nome": hierarquia["unidade"],
            "totalCriticos": 0,
            "totalComuns": 0,
            "detalhesCriticos": {
                "cpuCritico": 0,
                "ramCritico": 0,
                "discoCritico": 0,
                "redeCritico": 0,
            },
            "detalhes": {"cpu": 0, "ram": 0, "disco": 0, "rede": 0},
        }

    totalComuns = int(alertasCpu + alertasRam + alertasDisco + alertasRede)
    jsonHospital["unidades"][unidadeKey]["totalCriticos"] += int(criticos)
    jsonHospital["unidades"][unidadeKey]["totalComuns"] += int(totalComuns)
    jsonHospital["unidades"][unidadeKey]["detalhes"]["cpu"] += int(alertasCpu)
    jsonHospital["unidades"][unidadeKey]["detalhes"]["ram"] += int(alertasRam)
    jsonHospital["unidades"][unidadeKey]["detalhes"]["disco"] += int(alertasDisco)
    jsonHospital["unidades"][unidadeKey]["detalhes"]["rede"] += int(alertasRede)
    jsonHospital["unidades"][unidadeKey]["detalhesCriticos"]["cpuCritico"] += int(
        criticosCPU
    )
    jsonHospital["unidades"][unidadeKey]["detalhesCriticos"]["ramCritico"] += int(
        criticosRAM
    )
    jsonHospital["unidades"][unidadeKey]["detalhesCriticos"]["discoCritico"] += int(
        criticosDisco
    )
    jsonHospital["unidades"][unidadeKey]["detalhesCriticos"]["redeCritico"] += int(
        criticosRede
    )

    # Filtrando a mais crítica

    # Para cada nome de unidade X, compare os valores total de críticos
    unidadeMaisCritica = max(
        jsonHospital["unidades"].values(), key=lambda x: x["totalCriticos"]
    )
    jsonHospital["unidadeMaisCritica"] = unidadeMaisCritica

    # Calcula novamente os alertas semanais para acumular)
    jsonHospital["alertasSemanais"]["totalAlertas"] = sum(
        jsonHospital["alertasSemanais"]["porComponente"].values()
    )
    jsonHospital["criticos"]["totalCriticos"] = sum(
        jsonHospital["criticos"]["porComponente"].values()
    )

    listaUnidades = list(jsonHospital["unidades"].values())

    rankingUnidades = sorted(
        listaUnidades,
        key=lambda x: (x["totalCriticos"], x["totalComuns"]),
        reverse=True,
    )

    jsonHospital["rankingUnidades"] = rankingUnidades

    jsonHospital["unidadeMaisCritica"] = rankingUnidades[0]

    s3.put_object(
        Bucket=bucket,
        Key=caminhoJsonHospital,
        Body=json.dumps(
            jsonHospital, ensure_ascii=False
        ),  # ensure ascii garante que se houver acentos eles não serão substituídos
        ContentType="application/json",
    )
    # ========== FIM DA DASH DO DIEGO HENRIQUE ==========

    # ------------------------Dash Gustavo---------------------------------------------------------------------------------------
    caminhoJsonUnidade = f"{unidade_path}unidade.json"

    arquivoExisteUnidade = s3.list_objects_v2(Bucket=bucket, Prefix=caminhoJsonUnidade)


    if "Contents" in arquivoExisteUnidade:
        respostaUnidade = s3.get_object(Bucket=bucket, Key=caminhoJsonUnidade)
        jsonUnidade = json.loads(respostaUnidade["Body"].read().decode("utf-8"))
    else:
        jsonUnidade = {
            "id": id_unidade,
            "nome": hierarquia["unidade"],
            "listaMonitores": {},
            "trafegoRede": []
        }

    jsonUnidade.setdefault("listaMonitores", {})
    jsonUnidade.setdefault("trafegoRede", [])
    jsonUnidade.setdefault("monitoresEmAlertaLista", [])
    jsonUnidade.setdefault("monitoresAltoTempoUso", [])
    jsonUnidade.setdefault("monitoresGargaloRede", [])
    jsonUnidade.setdefault("monitoresConsumoRede", [])

    jsonUnidade["listaMonitores"][str(id_monitor)] = {
        "id": id_monitor,
        "ativo": monitor_ativo,
        "statusGeral": statusgeral,
        "tempoUsoHoras": round(intervalo / 60, 2),
        "cpu": cpu,
        "ram": ram,
        "disco": diskUsed,
        "rede": redeAtual,
        "redeAtual": redeAtual,
        "redePico": rede,
        "ultimaAtualizacao": horarioFim
    }

    jsonUnidade["ultimaAtualizacao"] = dataAtual.strftime("%Y-%m-%d %H:%M:%S")

    total = 0
    ativos = 0
    alerta = 0
    criticos = 0
    somaTempo = 0
    acima12 = 0
    gargaloRede = 0
    usoRedeUnidade = 0 
    jsonUnidade["monitoresEmAlertaLista"] = []
    jsonUnidade["monitoresAltoTempoUso"] = []
    jsonUnidade["monitoresGargaloRede"] = []
    jsonUnidade["monitoresConsumoRede"] = [] 
    maiorCpu = 0
    maiorRam = 0
    maiorDisco = 0

    lista = list(jsonUnidade["listaMonitores"].values())

    for mon in lista:

        total += 1

        if mon["ativo"] == True:
            ativos += 1

        if mon["statusGeral"] == "Alerta":
            alerta += 1

        if mon["statusGeral"] == "Crítico":
            criticos += 1

        somaTempo += mon["tempoUsoHoras"]
        usoRedeUnidade += float(mon.get("redeAtual", mon.get("rede", 0)) or 0)

        if mon["tempoUsoHoras"] >= 12:
            acima12 += 1
            jsonUnidade["monitoresAltoTempoUso"].append(mon)

        if mon["statusGeral"] == "Alerta" or mon["statusGeral"] == "Crítico":
            jsonUnidade["monitoresEmAlertaLista"].append(mon)

        if mon["cpu"] > maiorCpu:
            maiorCpu = mon["cpu"]

        if mon["ram"] > maiorRam:
            maiorRam = mon["ram"]

        if mon["disco"] > maiorDisco:
            maiorDisco = mon["disco"]

    if rede_total_unidade and rede_total_unidade > 0:
        usoRedePercentual = round((usoRedeUnidade / rede_total_unidade) * 100, 2)
    else:
        usoRedePercentual = 0

    if usoRedePercentual >= 90:
        gargaloRede = 1
        jsonUnidade["monitoresGargaloRede"] = [
            mon for mon in lista if float(mon.get("redeAtual", mon.get("rede", 0)) or 0) > 0
        ]
    else:
        gargaloRede = 0
        jsonUnidade["monitoresGargaloRede"] = []  

    jsonUnidade["monitoresAltoTempoUso"] = sorted(
        jsonUnidade["monitoresAltoTempoUso"],
        key=lambda x: x["tempoUsoHoras"],
        reverse=True,
    )

    jsonUnidade["monitoresConsumoRede"] = sorted(
        lista,
        key=lambda x: float(x.get("redeAtual", x.get("rede", 0)) or 0),
        reverse=True
    )

    jsonUnidade["gargaloRede"] = gargaloRede

    if total > 0:
        mediaTempo = somaTempo / total
    else:
        mediaTempo = 0

    jsonUnidade["totalMonitores"] = total
    jsonUnidade["monitoresAtivos"] = ativos
    jsonUnidade["emAlerta"] = alerta
    jsonUnidade["criticos"] = criticos
    jsonUnidade["tempoMedioUsoHoras"] = round(mediaTempo, 1)
    jsonUnidade["monitoresAcimaDeDozeHoras"] = acima12

    jsonUnidade["modulosAtivos"] = {}
    qtdModulosAtivos = 0
    for col in status_cols:
        jsonUnidade["modulosAtivos"][col] = ultimo[col]
        if ultimo[col] == "Ativo":
            qtdModulosAtivos += 1

    jsonUnidade["qtdModulosAtivos"] = qtdModulosAtivos

    jsonUnidade["trafegoRede"].append({
        "timestamp": horarioFim,
        "trafegoMbps": round(usoRedeUnidade, 2),
        "usoRedePercentual": usoRedePercentual
    })

    if len(jsonUnidade["trafegoRede"]) > 30:
        jsonUnidade["trafegoRede"] = jsonUnidade["trafegoRede"][-30:]


    jsonUnidade["usoRedeMbps"] = round(usoRedeUnidade, 2)
    jsonUnidade["usoRedePercentual"] = usoRedePercentual

    if usoRedePercentual >= 90:
        jsonUnidade["statusRedeUnidade"] = "Gargalo"
    elif usoRedePercentual >= 70:
        jsonUnidade["statusRedeUnidade"] = "Alerta"
    else:
        jsonUnidade["statusRedeUnidade"] = "OK"


    jsonUnidade["redeTotalMbps"] = rede_total_unidade
    

    jsonUnidade["recursos"] = {
        "cpu": {"pico": maiorCpu},
        "ram": {"pico": maiorRam},
        "disco": {"pico": maiorDisco},
    }

    jsonUnidade["ultimaAtualizacao"] = dataAtual.strftime("%Y-%m-%d %H:%M:%S")

    s3.put_object(
        Bucket=bucket,
        Key=caminhoJsonUnidade,
        Body=json.dumps(jsonUnidade, ensure_ascii=False),
        ContentType="application/json",
    )

    s3.put_object(
        Bucket=bucket,
        Key=caminhoJsonUnidade,
        Body=json.dumps(jsonUnidade, ensure_ascii=False),
        ContentType="application/json",
    )


    # ---------------------------fim dash Gustavo -------------------------------------------------------------------------------

    ########################################### Dash Maria:
    caminhoJsonMonitor = f"{caminho_monitor}"

    alertasCpu = 0
    alertasRam = 0
    alertasDisco = 0
    alertasRede = 0

    usoDiscoPercent = diskUsed * 100 / diskTotal
    usoDiscoPercentForm = round(usoDiscoPercent, 2)

    if df_client["cpu_percent"].iloc[-1] > limite_cpu:
        alertasCpu += 1

    if df_client["ram_percent"].iloc[-1] > limite_ram:
        alertasRam += 1

    if rede > limite_rede:
        alertasRede += 1

    if usoDiscoPercent > limite_disk:
        alertasDisco += 1

    totalAlertas = alertasCpu + alertasRam + alertasDisco + alertasRede

    print("Alertas totais do monitor: ", totalAlertas)

    # Gráficos
    valoresCpu = df_client["cpu_percent"].tail(10).tolist()
    valorRam = df_client["ram_percent"].tail(10).tolist()
    valorRede = df_client["banda_larga"].tail(10).tolist()
    horariosGrafico = df_client["timestamp"].tail(10).dt.strftime("%H:%M").tolist()

    arquivoExiste = s3.list_objects_v2(Bucket=bucket, Prefix=caminhoJsonMonitor)

    if "Contents" in arquivoExiste:  # Se retornar "Contents" ele já existe
        respostaS3 = s3.get_object(Bucket=bucket, Key=caminhoJsonMonitor)
        jsonMonitor = json.loads(respostaS3["Body"].read().decode("utf-8"))

        ultimaAtualizacao = df_client["timestamp"]

    else:
        jsonMonitor = {
            "id": id_monitor,
            "ativo": monitor_ativo,
            "horario": horarioFim,
            "cpu": {
                "usoCpuPercent": df_client["cpu_percent"].iloc[-1],
                "cpuPico": cpu,
            },
            "ram": {"usoRamPercent": df_client["ram_percent"].iloc[-1], "ramPico": ram},
            "disco": {
                "diskUsed": diskUsed,
                "diskTotal": diskTotal,
                "usoPercentualDisco": usoDiscoPercentForm,
            },
            "rede": {
                "picoMbs": rede,
                "upload": upload,
                "download": download,
                "trafegoTotal": trafego_total,
            },
            "limites": {
                "limiteCpu": limite_cpu,
                "limiteRam": limite_ram,
                "limiteDisco": limite_disk,
                "limiteRede": limite_rede,
            },
            "modulos": {col: ultimo[col] for col in status_cols},
            "alertasMonitor": {
                "alertasCpu": alertasCpu,
                "alertasRam": alertasRam,
                "alertasDisco": alertasDisco,
                "alertasRede": alertasRede,
                "alertasTotais": totalAlertas,
            },
            "graficos": {
                "valoresCpu": valoresCpu,
                "valorRam": valorRam,
                "valorRede": valorRede,
                "horariosGrafico": horariosGrafico
                },
        }

    s3.put_object(
        Bucket=bucket,
        Key=caminhoJsonMonitor,
        Body=json.dumps(
            jsonMonitor, ensure_ascii=False
        ),  # ensure ascii garante que se houver acentos eles não serão substituídos
        ContentType="application/json",
    )

    print("CLIENT atualizado")
    print(f"Empresa: {id_empresa}")
    print(f"Hospital: {id_hospital}")
    print(f"Unidade: {id_unidade}")
    print(f"Monitor: {id_monitor}")
    print(f"Modelo: {nome_modelo}")

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
