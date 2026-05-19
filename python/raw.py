import psutil
import time
import pandas as pd
from datetime import datetime
import os
import boto3
from botocore.exceptions import ClientError
import subprocess
import random
from random import randint
import sys
import logging
from dotenv import load_dotenv
from io import StringIO
import mysql.connector
from getpass import getpass

# Importando Bibliotecas Necessárias:
# psutil = Captura de Hardware e processos;
# time = Delay nas capturas para melhor análise e possibilidades;
# pandas as pd = Estrutura e manipulação de dados (DataFrame) e exportação para CSV;
# Datetime = Pegar a hora atual da Captura;
# OS = Funções para checar existência de pastas/arquivos;
# boto3 = Interagir com a AWS (s3);
# subprocess = Gerar processos fantasmas para simular modulos;
# random =  Utilizado da mesma forma que um rnorm para criar padrões;
# randomint = Gera um random entre intervalos;
# sys = Sendo usado para pegar o Python que está sendo executado no PC, evitando assim conflitos;
# logging = Debug e registrar erros;
# dotenv = Salvar credenciais em um .env para maior segurança e melhor manipulação das mesmas entre arquivos;
# StringIO = Salvar arquivos em buffer sem criar pasta local, para melhor manipulação em AWS;
# MYSQL.connector = Conexão com Banco de Dados (MySQL);
# GetPass = Salvar senha de forma segura.

# Função para limpar o Terminal de forma dinâmica entre Windows e Linux:
def limpar_terminal():
    os.system('cls' if os.name == 'nt' else 'clear')

header = [
    "id_monitor",
    "timestamp",
    "cpu_percent",
    "ram_percent",
    "disk_used",
    "disk_total",
    "bytes_sent_per_sec",
    "bytes_recv_per_sec",
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

# Criando Header com:
# "id_monitor" = ID do monitor atual;
# "timestamp" = Data e hora da coleta dos dados;
# "cpu_percent" = Porcentagem de CPU;
# "ram_percent" = Porcentagem da memória RAM;
# "ram_used_gb" = Ram sendo utilizada já em gigabytes;
# "disk_usage_percent" = Porcentagem do disco sendo utilizado atualmente;
# "bytes_sent_per_sec" = Velocidade média de envio (upload) em bytes por segundo;
# "bytes_recv_per_sec" = Velocidade média de recebimento (download) em bytes por segundo.
# "bpm_status": indica a quantidade de batimentos cardíacos por minuto; 
# "pa_status": Pressão arterial (PA): mede a força com que o sangue é bombeado pelo coração pode ser PNI (Pressão Não Invasiva) ou PN (Pressão Invasiva); 
# "spo2_status": Saturação periférica de oxigênio (SpO₂): mensura a quantidade de oxigênio no sangue; 
# "resp_status": Frequência respiratória: analisa a quantidade de respirações por minuto; 
# "temperatura_status": Temperatura corporal: mede a temperatura do corpo; 
# "pic_status": Pressão intracraniana: mensura a pressão no crânio; 
# "pvc_status": Pressão venosa central: mede a pressão nas veias centrais; 
# "ecg_status": Eletrocardiograma (ECG): avalia a atividade elétrica do coração; 
# "etco2_status": Capnografia (EtCO₂): mede a quantidade de dióxido de carbono no ar exalado. 

modulos = {
    "bpm": ["bpm_module.py"],
    "pa": ["pa_module.py"],
    "spo2": ["spo2_module.py"],
    "resp": ["resp_module.py"],
    "temperatura": ["temp_module.py"],
    "pic": ["pic_module.py"],
    "pvc": ["pvc_module.py"],
    "ecg": ["ecg_module.py"],
    "etco2": ["etco2_module.py"],
}

# Definindo os Módulos que serão capturados:
# Cada módulo representa um "módulo físico" do monitor multiparamétrico;
# Aqui estamos simulando esses módulos como processos rodando no sistema.

# Carregando dot env para acessar credenciais depois.
load_dotenv()

# Conexão com Banco de dados
conexao = mysql.connector.connect(
   host=os.getenv("host"),
    user=os.getenv("user"),
    password=os.getenv("password"),
    database=os.getenv("database")
)

# Cursor para executar Queryes
cursor = conexao.cursor(dictionary=True)

# Começando os Prints para pedir email e senha do usuario do Script de Captura
limpar_terminal()

print("=" * 60)
print("     SAINT PETER TECHNOLOGY")
print("        SISTEMA DE CAPTURA")
print("=" * 60)

print("\nBem-vindo ao sistema de monitoramento hospitalar.\n")

# Input de email e senha
email = input("Email: ")
senha = getpass("Senha: ")

# Query para buscar se o usuario existe
query_usuario = """
SELECT *
FROM usuarios
WHERE email = %s
AND senha = %s
"""

# Executando Query
cursor.execute(query_usuario, (email, senha))

# Pegando resultado da Query
usuario = cursor.fetchone()

# Checando se o usuario existe e caso não exista finalizando.
if not usuario:
    print("Usuário não encontrado!")
    exit()

# Recebendo usuario logado
print(f"\nLogin realizado com sucesso.")
print(f"Usuário: {usuario['nome_usuario']}")

# Dando select para retornar os monitores disponiveis baseado no usuario
query_monitores = """
SELECT 
    m.id_monitor,
    m.status_monitor,
    u.nome_unidade,
    h.nome_hospital,
    mm.nome
FROM monitores m
JOIN unidades u ON m.fk_unidade = u.id_unidade
JOIN hospitais h ON u.fk_hospital = h.id_hospital
JOIN modelos mm ON m.fk_modelo = mm.id_modelo
WHERE m.fk_empresa = %s AND m.status_monitor = 'Inativo';
"""

# Cursor passando o fk_empresa
cursor.execute(query_monitores, (usuario['fk_empresa'],))

# Resultado do cursor
monitores = cursor.fetchall()

if not monitores:
    print("Nenhum monitor encontrado.")
    exit()

print("\nMONITORES DISPONÍVEIS")

for i, monitor in enumerate((monitores), start=1):
    print(
        f"[{i}] - "
        f"Monitor: {monitor['id_monitor']}  |  "
        f"Hospital: {monitor['nome_hospital']}  |  "
        f"Unidade: {monitor['nome_unidade']}  |  "
        f"Modelo: {monitor['nome']}  |  "
        f"Status: {monitor['status_monitor']}" 
    )

try:
    opcao = int(input("\nSelecione o monitor: "))

    if opcao < 1 or opcao > len(monitores):
        print("Monitor inválido.")
        exit()

except ValueError:
    print("Digite apenas números.")
    exit()

monitor_escolhido = monitores[opcao - 1]

id_monitor = monitor_escolhido['id_monitor']

print(f"\nMonitor selecionado com sucesso.")
print(f"ID Monitor: {id_monitor}")

query_componentes = """
SELECT
    c.nome_componente,
    c.comando_psutil
FROM componente_monitor cm
JOIN componentes c
    ON cm.fk_componente = c.id_componente
WHERE cm.fk_monitor = %s
"""

cursor.execute(query_componentes, (id_monitor,))

componentes_monitor = cursor.fetchall()

query_update = """
UPDATE monitores
SET status_monitor = 'Ativo'
WHERE id_monitor = %s
"""
cursor.execute(query_update, (id_monitor,))
conexao.commit()


# Criando apra guardar processos fantasmas e ter um controle melhor deles.
processos_ativos = {}

#Criando pesos (normalizados) de probabilidade de acordo com R:

peso_dia = {
    0: 17.61,  
    1: 16.87,  
    2: 16.10,  
    3: 16.21,  
    4: 19.67,  
    5:  8.22,  
    6:  5.32   
}

peso_hora = {
    0: 4.06,  1: 2.71,  2:1.70,  3: 1.64,
    4: 1.95,  5: 2.00,  6:2.52,  7:12.07,
    8: 6.01,  9: 4.24, 10: 5.44, 11: 6.12,
    12:5.65, 13: 5.16, 14: 4.37, 15: 3.16,
    16:3.12, 17: 3.39, 18: 3.80, 19: 3.71,
    20:3.56, 21: 4.14, 22: 4.66, 23: 4.80
}


def simular_processos_fantasmas():
    # Função responsável por criar processos simulados (módulos)
    # com base em probabilidades dependentes de horário e dia da semana

    agora = datetime.now()
    hora = agora.hour
    dia_semana = agora.weekday()

    peso_d = peso_dia[dia_semana] / 100
    peso_h = peso_hora[hora] / 100

    peso_total = (peso_d + peso_h)/2


    #Probabilidade por módulo:
    probabilidades = {
        "ecg": 0.85,
        "spo2":0.80,
        "bpm": 0.80,
        "resp": 0.75,
        "temperatura":0.70,
        "pa": 0.65,
        "etco2": 0.55,
        "pvc": 0.30,
        "pic": 0.20,
    }

    #De acordo com o R, horarios com mais acidentes aumentam o uso de certos módulos:

    if dia_semana in (4, 5):
        probabilidades["pic"] = 0.35
        probabilidades["pvc"] = 0.45

    if hora >= 20 or hora < 4:
        probabilidades["pic"] = 0.40
        probabilidades["pvc"] = 0.50

    if dia_semana in (4, 5) and (hora >= 20 or hora < 4):
        probabilidades["pic"] = 0.55
        probabilidades["pvc"] = 0.55

# Toda criação de probabilidade de X ou Y modulo estar ativo
# feito com base em pesquisas de funciomamento hospitalar.
    
    for m in list(processos_ativos):
            if processos_ativos[m].poll() is not None:
                del processos_ativos[m]

# For para percorrer dicionario de processos_ativos e apagar processos
# que estão como "ativos" mas já foram terminados

# Criando um for dentro de um for para percorrer os modulos e pegar o nome deles e atribuir sua probabilidade
# criada anteriromente em seus dicionarios além disso ele checa se o processo já está ativo atribuindo mais chance
# de ele continuar ativo e tem um else para matar os processos fantasmas porem se if
# (random.random < prob) ele cria o processo fantasma.

    for modulo in modulos:
        prob = probabilidades.get(modulo, 0)
        ativo = modulo in processos_ativos

        if ativo:
            prob = min(prob * 1.2, 1)

        prob = min(prob * (0.5 + peso_total * 3), 1.0)

        if random.random() < prob:
            if not ativo:
                proc = subprocess.Popen(
                    [sys.executable, "-c",
                     f"import time; modulo='{modulo}';\nwhile True: time.sleep(1)"]
                )
                processos_ativos[modulo] = proc
        else:
            if ativo:
                proc = processos_ativos[modulo]
                try:
                    proc.terminate()
                    time.sleep(0.5)
                    if proc.poll() is None:
                        proc.kill()
                except Exception as e:
                    print(f"Erro ao matar processo: {e}")

                del processos_ativos[modulo]

def verificar_modulos():
    # Função responsável por verificar se os módulos estão ativos no sistema

    status_modulos = {}
    processos = list(psutil.process_iter(['cmdline']))

    # Buscando o nome dos modulos
    for nome_modulo in modulos:
        ativo = False

        for proc in processos:
            try:
                if proc.info['cmdline']:
                    comando = " ".join(proc.info['cmdline'])

                    if f"modulo='{nome_modulo}'" in comando:
                        ativo = True
                        break

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
            
        # Marcando como inativo ou ativo caso tenha ou não tenha encontrado o nome
        status_modulos[nome_modulo] = "Ativo" if ativo else "Inativo"

    return status_modulos

def coletar_componentes(componentes):
    dados = {}

    for componente in componentes:
        nome = componente['nome_componente']
        comando = componente['comando_psutil']

        if not comando:
            continue

        try:
            resultado = eval(f"psutil.{comando}")

            if nome == "Disco_Usado":
                dados["disk_used"] = resultado
                
            elif nome == "Disco_Total":
                dados["disk_total"] = resultado

            elif nome == "CPU":
                dados["cpu_percent"] = resultado

            elif nome == "RAM":
                dados["ram_percent"] = resultado

        except Exception as e:
            print(f"Erro ao coletar {nome}: {e}")

    return dados

try:
    while True:
        print("\nIniciando nova coleta de dados...")

        # Cria os Processos Fantasmas
        simular_processos_fantasmas()
        print("Simulação de processos atualizada")

        # Contador de Registros
        contador = 1

        # Define o caminho completo do arquivo CSV dentro da pasta criada.

        buffer_csv = StringIO()
        df_init = pd.DataFrame(columns=header)
        df_init.to_csv(buffer_csv, index=False)

        print("Buffer CSV inicializado com cabeçalho\n")

        # Cria o arquivo CSV com apenas o cabeçalho caso ele ainda não exista.

        print("Iniciando ciclo de capturas (10 registros): \n")
        while contador <= 20:
            # Início do loop infinito para captura contínua dos dados do sistema:

            dados_componentes = coletar_componentes(componentes_monitor)

            cpu = dados_componentes.get("cpu_percent", 0)
            ram = dados_componentes.get("ram_percent", 0)
            disk_used = dados_componentes.get("disk_used", 0)
            disk_total = dados_componentes.get("disk_total", 0)

            date = datetime.now().strftime('%d-%m-%Y %H_%M_%S')
            # Captura o timestamp atual da coleta formatado;

            net = psutil.net_io_counters()
            # Captura os bytes totais enviados e recebidos até o momento;

            bytesSend1 = net.bytes_sent
            bytesRecv1 = net.bytes_recv
            # Armazena o primeiro ponto de medição da rede;

            time.sleep(5)
            # Aguarda 5 segundos para calcular a variação de tráfego de rede;

            net = psutil.net_io_counters()

            bytesSend2 = net.bytes_sent
            bytesRecv2 = net.bytes_recv
            # Segundo ponto de medição da rede;

            # Calcula a taxa média de envio e recebimento por segundo;
            bytes_sent_per_sec = (bytesSend2 - bytesSend1) / 5
            bytes_recv_per_sec = (bytesRecv2 - bytesRecv1) / 5

            # Captura o status dos módulos simulados
            modulos_status = verificar_modulos()

            # Criando dicionario para pegar os processos realmente ativos no momento

            atuais = []

            for modulo in modulos:
                if(modulos_status[modulo] == "Ativo"):
                    atuais.append(modulo)


            n_ativos = len(atuais)

            # Aumentando os valores para cada módulo ativo

            carga = peso_hora[datetime.now().hour] / 12.07
            bytes_sent_per_sec = bytes_sent_per_sec * (1.0 - carga * 0.4)
            bytes_recv_per_sec = bytes_recv_per_sec * (1.0 - carga * 0.4)

            bytes_sent_per_sec = bytes_sent_per_sec / (1 + n_ativos * 3.0)
            bytes_recv_per_sec = bytes_recv_per_sec / (1 + n_ativos * 3.0)


            ram += n_ativos * random.uniform(0.7, 1.5)
            cpu += n_ativos * random.uniform(2.0,3.5)

            #Freio para não passar muito de 80%

            if ram > 80:
                passou = ram - 80
                ram = 80 + passou * 0.2

            if cpu > 80:
                passou = cpu - 80
                cpu = 80 + passou * 0.2

            # Cria uma lista com os dados coletados
            linha = [
                id_monitor,
                date,
                round(cpu, 2),
                round(ram, 2),
                round(disk_used, 0),
                round(disk_total, 0),
                round(bytes_sent_per_sec, 2),
                round(bytes_recv_per_sec, 2),
            ]

            # Adiciona o status dos módulos na linha
            for modulo in modulos:
                linha.append(modulos_status[modulo])
                
            # Cria um DataFrame com uma única linha contendo os dados coletados.
            df = pd.DataFrame([linha], columns=header)

            # Salva os dados no CSV no modo append (sem sobrescrever o arquivo);
            df.to_csv(buffer_csv, mode='a', header=False, index=False)

            redeTotal = bytes_sent_per_sec + bytes_recv_per_sec

            limpar_terminal()

            print("=" * 60)
            print("      SAINT PETER TECHNOLOGY - MONITORAMENTO")
            print("=" * 60)

            print(f"Monitor: {id_monitor}")
            print(f"Captura: {contador}/20")
            print(f"Horário: {date}")

            print("-" * 60)

            print(f"CPU         : {cpu:.2f}%")
            print(f"RAM         : {ram:.2f}%")
            print(f"DISCO       : {disk_used}bytes")
            print(f"DISCO TOTAL : {disk_total}bytes")
            print(f"BROADBAND   : {redeTotal:.2f}mb/s")

            print("-" * 60)

            print("MÓDULOS ATIVOS:")

            for modulo in atuais:
                print(f"  ✓ {modulo.upper()}")

            print("=" * 60)

            time.sleep(60)
            # Aguarda mais 60 segundos antes da próxima coleta (controle de frequência).

            contador += 1

        print("\nFinalizando as 20 capturas, preparando envio\n")

        # Envia para o S3
        s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("aws_access_key_id"),
        aws_secret_access_key=os.getenv("aws_secret_access_key"),
        aws_session_token=os.getenv("aws_session_token")
        )

        nome_arquivo = f"M{id_monitor}_{datetime.now().strftime('%d-%m-%Y_%H_%M')}.csv"

        buffer_csv.seek(0)

        print("Enviando arquivo para o bucket...")

        response = s3_client.put_object(
            Bucket=os.getenv("bucket"),
            Key="raw/" + nome_arquivo,
            Body=buffer_csv.getvalue().encode('utf-8'),
            ContentType='text/csv'
        )

        print(f"Upload concluído: {nome_arquivo}\n")

        

except Exception as e:
    print(f"Erro no sistema: {e}")
finally:
    query_update = """
    UPDATE monitores
    SET status_monitor = 'Inativo'
    WHERE id_monitor = %s
    """
    cursor.execute(query_update, (id_monitor,))
    conexao.commit()
    
    for modulo, proc in processos_ativos.items():
        try:
            proc.terminate()
            proc.wait(timeout=2)
        except:
            proc.kill()

    cursor.close()
    conexao.close()

    print("Sistema encerrado.")
