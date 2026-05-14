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


class Raft():
    def __init__(self, id):
        self._id = id
        self._isleader = False

        self._voted_for = None
        self._votes = 0
        self._last_heartbeat = time.time()
        self._time_out = random.uniform(0.5, 0.75)

        self._running = True
        self._id_last_leader = None
        self._term = 0

        self._count_n_process = 0

        self._lock = threading.Lock()

        self.state = "FOLLOWER"

    def get_current_time(self):
        return datetime.now().strftime("%H:%M:%S.%f")[:-3]

    @Pyro5.api.expose
    def request_vote(self, candidate_term, candidate_id):

        with self._lock:
            # candidato está atrasado
            if candidate_term < self._term:
                print(f'| {self.get_current_time()} | Termo do candidato {candidate_id} - {candidate_term} MENOR que o MEU - {self._id}')
                return 0

            # encontrou term maior
            if candidate_term > self._term:
                print(f'| {self.get_current_time()} | Termo do candidato {candidate_id} - {candidate_term} MAIOR que o MEU - {self._id}')
                self._term = candidate_term
                self._voted_for = None
                self._isleader = False
                
            # já votei nesse term
            if self._voted_for not in [None, candidate_id]:
                print(f'| {self.get_current_time()} | Já VOTEI nesse cara {candidate_id}')
                return 0

            self._voted_for = candidate_id
            self._last_heartbeat = time.time()

            print(f'| {self.get_current_time()} | Process {self._id} votou em {candidate_id}')

            return 1

    @Pyro5.api.expose
    def heartbeat(self, leader_id, leader_term):
        with self._lock:
            # ignora líderes atrasados
            if leader_term < self._term:
                print(f'| {self.get_current_time()} | Processo {leader_id} ATRASADO')
                return

            if leader_term > self._term:
                self._term = leader_term
                self._voted_for = None

            self._isleader = False

            self._last_heartbeat = time.time()
            self._time_out = random.uniform(0.5, 0.75)

            self._id_last_leader = leader_id
            self.state = "FOLLOWER"



            

        print(f'| {self.get_current_time()} | Process {self._id} recebeu heartbeat do lider {leader_id}')


    @Pyro5.api.expose
    def commit(self):
        pass

    def init_application(self):
        while self._running:

            if self.state == "FOLLOWER":
                self.run_follower()

            elif self.state == "CANDIDATE":
                self.run_candidate()

            elif self.state == "LEADER":
                self.run_leader()

    def became_leader(self):
        with self._lock:
            self.state = "LEADER"
            self._isleader = True
            self._votes = 0
            self._voted_for = self._id

        servidor_nomes = Pyro5.api.locate_ns(host="localhost", port=9095)

        try:
            servidor_nomes.remove("lider")
        except:
            pass

        servidor_nomes.register("lider", list_uri[self._id])

        print(f'| {self.get_current_time()} | Process {self._id} virou LIDER')

    def became_follower(self):
        with self._lock:
            self.state = "FOLLOWER"
            self._isleader = False
            self._votes = 0
            self._last_heartbeat = time.time()

        print(f'| {self.get_current_time()} | Process {self._id} virou FOLLOWER')


    def run_follower(self):
        elapsed = time.time() - self._last_heartbeat

        if elapsed > self._time_out:
            with self._lock:
                self.state = "CANDIDATE"
                self._last_heartbeat = time.time()
                self._term += 1
                self._voted_for = self._id
                self._votes = 1

            print(f'| {self.get_current_time()} | Process {self._id} virou CANDIDATO')

        time.sleep(0.05)

    def run_candidate(self):

        ns = Pyro5.api.locate_ns(host="localhost", port=9095)
        processos = ns.list()

        for nome, uri in processos.items():

            if nome in ["lider", "Pyro.NameServer"]:
                continue

            if nome == f"process{self._id}":
                continue

            try:
                process_proxy = Pyro5.api.Proxy(uri)
                if process_proxy.request_vote(self._term, self._id):

                    with self._lock:
                        self._votes += 1

                    self._count_n_process += 1

                    print(f'| {self.get_current_time()} | recebeu voto de {nome}')

            except:
                pass

        total_nodes = len(list_uri) - 3
        majority = (total_nodes // 2) + 1
            
        with self._lock:
            if self.state != "CANDIDATE":
                return

        if self._votes >= majority:
            self.became_leader()

        else:
            print(f'| {self.get_current_time()} | Processo {self._id} perdeu ELEIÇÃO')
            with self._lock:
                self._votes = 0
                self._voted_for = None
                self._last_heartbeat = time.time()

            self.became_follower()

        time.sleep(random.uniform(0.3, 0.6))

    def run_leader(self):

        ns = Pyro5.api.locate_ns(host="localhost", port=9095)
        processos = ns.list()

        for nome, uri in processos.items():
            if nome in ["lider", "Pyro.NameServer"]:
                continue

            if nome == f"process{self._id}":
                continue

            try:
                process_proxy = Pyro5.api.Proxy(uri)
                process_proxy.heartbeat(self._id, self._term)

            except:
                pass

        time.sleep(0.08)

n_process = int(input("nº do processo: "))

daemon = Pyro5.api.Daemon(port=9090 + n_process)
objeto_process = Raft(n_process)

uri = daemon.register(objeto_process, objectId=f"process{n_process}")

servidor_nomes = Pyro5.api.locate_ns(host="localhost", port=9095)
servidor_nomes.register(f"process{n_process}", uri)


print('uri: ', uri)

threading.Thread(target=objeto_process.init_application, daemon=True ).start()

daemon.requestLoop()







