import Pyro5.api
import threading

import time, random
from datetime import datetime

import json

list_uri = [
   "PYRO:process0@localhost:9090",
   "PYRO:process1@localhost:9091",
   "PYRO:process2@localhost:9092",
   "PYRO:process3@localhost:9093",
]

def get_current_time():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

#consistencia, os processos funcionam como um backup, só comitam após o lider permitir e quais permitir, sempre tem um cópia dos logs, todos tem o mesmo número de logs. 
# Verifica seus logs a partir do que o lider propoe, se faltar algo ele deve pedir ao lider novamente
class Raft():
    def __init__(self, id):
        self._id = id

        self._voted_for = None
        self._votes = 0
        self._last_heartbeat = time.time()
        self._time_out = random.uniform(3.0, 5.0)

        self._running = True
        self._term = 0

        self._count_n_process = 0

        self._state = "FOLLOWER"
        self._lock = threading.Lock()

        self._log = []
        self._flag = False

        self._current_index = -1 
        self._commmit = -1

        self._list_proxy = []

    def abre_arquivo_logs(self, id):
        with open(f"{id}_logs.json", "r", encoding="utf-8") as arquivo:
            conteudo = arquivo.read().strip()

            if not conteudo:
                return []

            return json.loads(conteudo)

    @Pyro5.api.expose
    def request_vote(self, candidate_term, candidate_id):
        if candidate_term < self._term:
            return 0

        if candidate_term > self._term:
            with self._lock:
                self._term = candidate_term
                self._voted_for = None       #pode ter votado em outra pessoa
                self._state = "FOLLOWER"

        if self._voted_for is None or self._voted_for == candidate_id: #se já votei anteriormente nele
            with self._lock:
                self._voted_for = candidate_id
            return 1

        return 0
        
    @Pyro5.api.expose
    def heartbeat(self, leader_term, leader_id, entry, leader_index, commit):
        with self._lock:
            if leader_term < self._term:
                return None
            
            self._term = leader_term
            self._state = "FOLLOWER"
            self._last_heartbeat = time.time()

            # heartbeat vazio
            if entry is None and commit < 0:
                print(f'| {get_current_time()} | Heartbeat')
                return True

            if entry:
                for item in entry:
                    index = item["index"]
                    mensagem = item["mensagem"]


                    if index == len(self._log):
                        self._log.append({
                            "term": self._term,
                            "mensagem": mensagem,
                            "commit": False,
                            "nome": self._id,
                            "current_time": get_current_time()
                        })

                    else:
                        self._log[index] = {
                            "term": self._term,
                            "mensagem": mensagem,
                            "commit": False,
                            "nome": self._id,
                            "current_time": get_current_time()
                        }

                print(f'| {get_current_time()} | Process {self._id} recebeu log {leader_index}')

            # follower está atrasado
            if leader_index > len(self._log):
                print(f'Process {self._id} está faltando logs. \nEsperado {commit} 'f'recebido {leader_index}')
                return commit

            if commit > -1:
                for index in range(commit + 1):
                        if self._log[index]["commit"] is not True:
                            self._log[index]["commit"] = True

                            self.commit_log(index)
                            print('Vamos comitar')

            print(f' | {get_current_time()} | retornou sem nada')

            return True

        # print(f'| {get_current_time()} | Recebeu heartbeat de {leader_id}')
        
    @Pyro5.api.expose
    def append_logs(self, mensagem):
        with self._lock:
            self._log.append({
                "term": self._term,
                "mensagem": mensagem,
                "commit": False,
                "nome": self._id,
                "current_time": get_current_time()
            })

            self._flag = True
            self._current_index = len(self._log) - 1

        print(f'| {get_current_time()} | Novo comando: {mensagem}')
        while self._flag:
            time.sleep(1)
            
        return "Comando recebido"

    def init_application(self):
        while self._running:
            if self._state == "FOLLOWER":
                self.run_follower()

            if self._state == "CANDIDATE":
                self.run_candidate()

            if self._state == "LEADER":
                self.run_leader()

            time.sleep(0.05)

    def run_follower(self):
        elapsed = time.time() - self._last_heartbeat

        if elapsed > self._time_out:
            with self._lock:
                self._state = "CANDIDATE"

            print(f'| {get_current_time()} | Process {self._id} virou CANDIDATO')

    def run_candidate(self):
        with self._lock:
            self._votes = 1
            self._term += 1
            self._voted_for = self._id
            current_term = self._term

        ns = Pyro5.api.locate_ns(host="localhost", port=9095)
        processos = ns.list()

        ativos = []

        for nome, uri in processos.items():
            if not nome.startswith("process"):
                continue

            if nome == f"process{self._id}":
                continue

            try:
                proxy = Pyro5.api.Proxy(uri)
                proxy._pyroBind()
                ativos.append((nome, proxy))

            except:
                print(f"Removendo processo morto {nome}")

                try:
                    ns.remove(nome)
                except:
                    pass

        total_processos = len(ativos) + 1
        maioria = (total_processos // 2) + 1

        for nome, proxy in ativos:
            try:
                voto = proxy.request_vote(current_term ,self._id)

                with self._lock:
                    if self._state != "CANDIDATE":
                        return

                    if voto > 0:
                        self._votes += 1

                        print(f'| {get_current_time()} | 'f'Recebeu voto de {nome}')

                        if self._votes >= maioria:
                            self._state = "LEADER"

                            print(f'| {get_current_time()} | 'f'Virou LIDER')
                            return

            except Exception as error:
                print(error)

        if self._votes >= maioria:
            self._state = "LEADER"

            print(f'| {get_current_time()} | 'f'Virou LIDER')
            return
        
        with self._lock:
            if self._state == "CANDIDATE":
                self._state = "FOLLOWER"
                self._votes = 0
                self._voted_for = None
                self._last_heartbeat = time.time()

                print(f'| {get_current_time()} | 'f'Virou SEGUIDOR')

        time.sleep(random.uniform(0.3, 0.8))

    def get_ative_process(self, ns):
        processos = ns.list()
        ativos = []

        for nome, uri in processos.items():
            if not nome.startswith("process"):
                continue

            if nome == f"process{self._id}":
                continue

            try:
                proxy = Pyro5.api.Proxy(uri)
                proxy._pyroBind()
                ativos.append((nome, proxy))

            except:
                # print(f"Removendo processo morto {nome}")

                try:
                    ns.remove(nome)
                except:
                    pass

        return ativos        

    def run_leader(self):
        ns = Pyro5.api.locate_ns(host="localhost", port=9095)
        processos = ns.list()

        try:
            ns.remove("lider")
        except:
            pass

        ns.register("lider", list_uri[self._id])

        if self._flag:
            success = 1
            entry = [
                {
                    "index": self._current_index,
                    "mensagem": self._log[self._current_index]["mensagem"]
                }
            ]

            send = [self._term, self._id, entry, self._current_index, self._commmit]

        else:
            print(f" | {get_current_time()} | Enviou heartbeat", self._commmit)
            entry = None
            success = 0

            send = [self._term, self._id, None, -1, self._commmit]

        total_processos = len(self.get_ative_process(ns)) + 1
        maioria = (total_processos // 2) + 1

        for nome, uri in processos.items():
            if nome == 'lider':
                continue

            if not nome.startswith("process"):
                continue

            if nome == f"process{self._id}":
                continue

            try:
                send_local = send.copy()

                if self._list_proxy:
                    print('Processos atrasados')
                    for item in self._list_proxy:
                        if item["nome"] == nome:
                            posicao_faltante = item["posicao_faltante"]
                            mensagem_faltante = self._log[posicao_faltante]["mensagem"]

                            entry = [
                                {
                                    "index": posicao_faltante,
                                    "mensagem": mensagem_faltante
                                },
                            
                                {
                                    "index": self._current_index,
                                    "mensagem": self._log[self._current_index]["mensagem"]
                                } 
                            ]

                            send_local[2] = entry
                           
                            break

                process_proxy = Pyro5.api.Proxy(uri)
                response = process_proxy.heartbeat(*send_local)

                if response == True:
                    success += 1
                    self._list_proxy = [
                        item for item in self._list_proxy
                        if item["nome"] != nome
                    ]

                elif response == None:
                    pass

                else:
                    self._list_proxy.append({
                        "nome": nome,
                        "posicao_faltante": response
                    })
                        
                # print(f'| {get_current_time()} | Enviou H T B para {nome}')

            except Exception as error:
                print('error: ', error)
                print(f'nao encontrado {nome} - {uri}')
                pass
 
        with self._lock:
            if self._flag and success >= maioria:
                self._log[self._current_index]["commit"] = True
                self.commit_log(self._current_index)
                # self._log[self._current_index]["committed"] = True
                self._flag = False
                self._commmit = self._current_index

        
        time.sleep(0.100)

        
    def commit_log(self, index):
        try:
            logs = self.abre_arquivo_logs(self._id)
            logs.append(self._log[index])

            with open(f"{self._id}_logs.json", "w", encoding="utf-8") as arquivo:
                json.dump(logs, arquivo,indent=4,ensure_ascii=False)

            print(f'| {get_current_time()} | Logs commitados com sucesso')

        except Exception as error:
            print(f'| {get_current_time()} | Erro ao salvar logs')
            print(error)


n_process = int(input("nº do processo: "))

daemon = Pyro5.api.Daemon(port=9090 + n_process)
objeto_process = Raft(n_process)

uri = daemon.register(objeto_process, objectId=f"process{n_process}")

servidor_nomes = Pyro5.api.locate_ns(host="localhost", port=9095)
servidor_nomes.register(f"process{n_process}", uri)

print('uri: ', uri)

threading.Thread(target=objeto_process.init_application, daemon=True ).start()

daemon.requestLoop()