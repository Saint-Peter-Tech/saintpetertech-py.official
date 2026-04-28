import psutil
import time
import pandas as pd
from datetime import datetime
import os
import subprocess
import random
from random import randint
import sys
import os

random.seed(42)


# Importando Bibliotecas Necessárias:
# psutil = Captura de Hardware e processos;
# time = Delay nas capturas para melhor análise e possibilidades;
# pandas as pd = Estrutura e manipulação de dados (DataFrame) e exportação para CSV;
# Datetime = Pegar a hora atual da Captura;
# OS = Funções para checar existência de pastas/arquivos;
# subprocess = Gerar processos fantasmas para simular modulos;
# random =  Utilizado da mesma forma que um rnorm para criar padrões.
# randomint = Gera um random entre intervaloos
# sys = Sendo usado para pegar o Python que está sendo executado no PC, evitando assim conflitos


header = [
    "id_monitor",
    "timestamp",
    "cpu_percent",
    "ram_percent",
    "disk_usage_percent",
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
# Cada módulo representa um "módulo físico" do monitor multiparamétrico
# Aqui estamos simulando esses módulos como processos rodando no sistema

pasta = './dados_brutos'

# Criando o caminho da pasta para salvar o CSV.

os.makedirs(pasta, exist_ok=True)


# ID do Monitor MUDAR SEMPRE!!!  
id_monitor = 1

# Cria a pasta caso não exista (evita erro ao salvar arquivo).

arquivoCSV = f"{pasta}/M{id_monitor} - {datetime.now().strftime('%d-%m-%Y %H_%M')}.csv"
pasta_processos = "./processos_fantasmas"

# Cria a pasta para os Processos Fantasmas (modulos) caso ela não exista.
os.makedirs(pasta_processos, exist_ok=True)


# Define o caminho completo do arquivo CSV dentro da pasta criada.

if not os.path.exists(arquivoCSV):
    df_init = pd.DataFrame(columns=header)
    df_init.to_csv(arquivoCSV, index=False)

# Cria o arquivo CSV com apenas o cabeçalho caso ele ainda não exista.

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

    for modulo, arquivos in modulos.items():
        prob = probabilidades.get(modulo, 0)

        ativo = modulo in processos_ativos

        if ativo:
            prob = min(prob * 1.2, 1)

        prob = min(prob * (0.5 + peso_total * 3), 1.0)

        if random.random() < prob:
            if not ativo:
                for script in arquivos:
                        caminho = os.path.join(pasta_processos, script)

                        if not os.path.exists(caminho):
                            with open(caminho, "w") as f:
                                f.write("import time\nwhile True:\n    time.sleep(1)")

                        proc = subprocess.Popen(
                            [sys.executable, caminho],
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

    # Captura todos os processos em execução no sistema
    processos = list(psutil.process_iter(['cmdline']))

    for nome_modulo, nomes_processos in modulos.items():

        ativo = False

        for proc in processos:
            try:
                if proc.info['cmdline']:
                    comando = " ".join(proc.info['cmdline'])

                    # Verifica se o nome do script está presente no processo
                    for nome in nomes_processos:
                        if nome in comando:
                            ativo = True
                            break

                if ativo:
                    break

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        # Define o status do módulo
        status_modulos[nome_modulo] = "Ativo" if ativo else "Inativo"

    return status_modulos

try:
    while True:

        # Cria os Processos Fantasmas
        simular_processos_fantasmas()

        # Pega o inicio do While
        inicio = time.time()

        # Gera um intervalo aleatorio para troca de módulos
        intervalo = 450

        while time.time() - inicio < intervalo:
            # Início do loop infinito para captura contínua dos dados do sistema:

            mem = psutil.disk_usage('/')
            # Captura informações de uso do disco (total, usado, livre e porcentagem);

            ram = psutil.virtual_memory().percent
            # Captura informações da memória RAM (porcentagem)

            cpu = psutil.cpu_percent(intervalo=1)
            # Captura o uso percentual da CPU (intervalo de 1 segundo para média mais precisa);

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
                    atuais.append(modulos_status[modulo])


            # Aumentando os valores para cada módulo ativo

            carga = peso_hora[datetime.now().hour] / 12.07
            bytes_sent_per_sec = bytes_sent_per_sec * (1.0 - carga * 0.4)
            bytes_recv_per_sec = bytes_recv_per_sec * (1.0 - carga * 0.4)
            for atual in atuais:
                if(cpu < 80):
                    cpu += 5
                else: 
                    cpu = 75
                if(ram < 80):
                    ram += 5
                else:
                    ram = 75

            # Cria uma lista com os dados coletados
            linha = [
                id_monitor,
                date,
                cpu,
                ram,
                mem.percent,
                bytes_sent_per_sec,
                bytes_recv_per_sec,
            ]

            # Adiciona o status dos módulos na linha
            for modulo in modulos:
                linha.append(modulos_status[modulo])
                
            # Cria um DataFrame com uma única linha contendo os dados coletados.
            df = pd.DataFrame([linha], columns=header)

            # Salva os dados no CSV no modo append (sem sobrescrever o arquivo);
            df.to_csv(arquivoCSV, mode='a', header=False, index=False, encoding='utf-8')

            time.sleep(60)
            # Aguarda mais 60 segundos antes da próxima coleta (controle de frequência).

except KeyboardInterrupt:
    print("Encerrando Monitoramento...")
    
    for modulo, proc in processos_ativos.items():
        try:
            proc.terminate()
            proc.wait(timeout=2)
        except:
            proc.kill()

    print("Todos os processos finalizados com sucesso!\nPrograma finalizado.")