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

        self._voter_for = None
        self._votes = 0
        self._last_heartbeat = time.time()
        self._time_out = random.uniform(0.500, 0.750)

        self._running = True
        self._id_last_leader = 0
        self._term = 1

    @Pyro5.api.expose
    def request_vote(self, id_candidate):
        if self._voter_for is None:
            self._voter_for = id_candidate

            hora = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            print(f' | {hora} | Process {self._id} votou em {id_candidate}')
            return 1
        
        return 0


    @Pyro5.api.expose
    def heartbeat(self, leader_id):
        self._last_heartbeat = time.time()

        if leader_id != self._id_last_leader:
            self._id_last_leader = leader_id
            self._term += 1
            self._voter_for = None
            hora = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            print(f'| {hora} | Process {self._id} NOVO LIDER DETECTADO {leader_id}')

        hora = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f'| {hora} | Process {self._id} recebeu heartbeat do lider {leader_id}')


    @Pyro5.api.expose
    def commit(self):
        pass

    def became_leader(self):
        self._isleader = True
        self._votes = 0

        hora = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f' | {hora} | Process {self._id} virou LIDER')

        threading.Thread(
            target=self.run_leader,
            daemon=True
        ).start()

    def became_follower(self):
        self._isleader = False
        self._votes = 0

        hora = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f' | {hora} | Process {self._id} virou SEGUIDOR')

        threading.Thread(
            target=self.run_follower,
            daemon=True
        ).start()
    

    def run_leader(self):
        start = time.time()

        while self._running:
            if time.time() - start >20:
                self._running = False
                self._isleader = False

                hora = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                print(f'| {hora} | LIDER CAIU {self._id}')
                break

            for i, process_uri in enumerate(list_uri):
                if i == self._id:
                    continue

                process_proxy = Pyro5.api.Proxy(process_uri) 
                process_proxy.heartbeat(self._id)

            time.sleep(0.500)

    def run_follower(self):
        while self._running:
            elapsed = time.time() - self._last_heartbeat

            if elapsed > self._time_out:
                hora = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                print(f'| {hora} | Process {self._id} virou CANDIDATO ')
                self._votes = 1
                self._voter_for = self._id

                for i, process_uri in enumerate(list_uri):
                    if i == self._id:
                        continue

                    process_proxy = Pyro5.api.Proxy(process_uri)
                    self._votes += process_proxy.request_vote(self._id)

                if self._votes >= 2:
                    self._isleader = True
                    self._votes = 0
                    self._voter_for = None
                    self._term += 1

                    hora = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    print(f'| {hora} | Process {self._id} VIROU LIDER')
                    self.became_leader()
                    break

                else:
                    self._votes = 0
                    self._voter_for = None
                    self._last_heartbeat = time.time()
                    self._time_out = random.uniform(0.500, 0.750)

                    hora = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    print(f' | {hora} | Process {self._id} PERDER ELEIÇÃO')


n_process = int(input("nº do processo: "))

daemon = Pyro5.api.Daemon(port=9090 + n_process)
objeto_process = Raft(n_process)

uri = daemon.register(objeto_process, objectId=f"process{n_process}")


print('uri: ', uri)

if n_process > 2:
    print('Processo iniciado \n\n')
    objeto_process.became_leader()

else:
    print('Processo iniciado \n\n')
    objeto_process.became_follower()


daemon.requestLoop()
