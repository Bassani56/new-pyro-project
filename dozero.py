import Pyro5.api
import threading

import time, random
from datetime import datetime

list_uri = [
   "PYRO:process0@localhost:9090",
   "PYRO:process1@localhost:9091",
   "PYRO:process2@localhost:9092",
   "PYRO:process3@localhost:9093",
]

def get_current_time():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

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
        self._flag = True

        self._state = "FOLLOWER"

        self._lock = threading.Lock()

    @Pyro5.api.expose
    def request_vote(self, candidate_term, candidate_id):

        self._last_heartbeat = time.time()

        if candidate_term > self._term:
            self._term = candidate_term
            self._voted_for = None
            self._state = "FOLLOWER"

        if self._voted_for in [None, candidate_id]:
            self._voted_for = candidate_id
            self._last_heartbeat = time.time()

            return 1

        return 0
        
    @Pyro5.api.expose
    def heartbeat(self, leader_term, leader_id):
        if leader_term < self._term:
            return
        
        with self._lock:
            self._term = leader_term
            self._state = "FOLLOWER"
            self._last_heartbeat = time.time()

        print(f'| {get_current_time()} | Recebeu heartbeat de {leader_id}')

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

        with self._lock:
            if self._state == "CANDIDATE":
                self._state = "FOLLOWER"
                self._votes = 0
                self._voted_for = None
                self._last_heartbeat = time.time()

                print(f'| {get_current_time()} | 'f'Virou SEGUIDOR')

        time.sleep(random.uniform(0.3, 0.8))

    def run_leader(self):
        ns = Pyro5.api.locate_ns(host="localhost", port=9095)
        processos = ns.list()

        for nome, uri in processos.items():
            # print('nome: ', nome)
            if nome == 'lider':
                continue

            if not nome.startswith("process"):
                continue

            if nome == f"process{self._id}":
                continue

            try:
                process_proxy = Pyro5.api.Proxy(uri)
                process_proxy.heartbeat(self._term, self._id)
                print(f'| {get_current_time()} | Enviou H T B para {nome}')

            except Exception as error:
                print('error: ', error)
                print(f'nao encontrado {nome} - {uri}')
                pass

        # print('fez porra nenhuma')    
        time.sleep(0.100)

n_process = int(input("nº do processo: "))

daemon = Pyro5.api.Daemon(port=9090 + n_process)
objeto_process = Raft(n_process)

uri = daemon.register(objeto_process, objectId=f"process{n_process}")

servidor_nomes = Pyro5.api.locate_ns(host="localhost", port=9095)
servidor_nomes.register(f"process{n_process}", uri)

print('uri: ', uri)

threading.Thread(target=objeto_process.init_application, daemon=True ).start()

daemon.requestLoop()







