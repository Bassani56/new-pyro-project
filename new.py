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
        self._id_last_leader = None
        self._term = 1


        self._count_n_process = 0


        self._flag = True


    def get_current_time(self):
        return datetime.now().strftime("%H:%M:%S.%f")[:-3]


    @Pyro5.api.expose
    def request_vote(self, id_candidate):
        if self._voter_for is None and self._flag:
            self._voter_for = id_candidate


            print(f' | {self.get_current_time()} | Process {self._id} votou em {id_candidate}')
            return 1
        
        return 0

    @Pyro5.api.expose
    def heartbeat(self, leader_id):
        self._last_heartbeat = time.time()


        if leader_id != self._id_last_leader:
            if self._id_last_leader is not None:
                print(f'| {self.get_current_time()} | Process {self._id} NOVO LIDER DETECTADO {leader_id}')


            self._id_last_leader = leader_id
            self._term += 1
            self._voter_for = None
            
            
            self._flag = False
        
        print(f'| {self.get_current_time()} | Process {self._id} recebeu heartbeat do lider {leader_id}')


    @Pyro5.api.expose
    def commit(self):
        pass


    def became_leader(self):
        self._isleader = True
        self._votes = 0


        try:
            servidor_nomes = Pyro5.api.locate_ns(host="localhost", port=9095)
            servidor_nomes.register("lider", list_uri[self._id])
            print(f"Processo {self._id} registrado no Name Server com URI {list_uri[self._id]}")


        except Exception as e:
            print(f'Erro ao registrar no Name Server: {e}')


        print(f' | {self.get_current_time()} | Process {self._id} virou LIDER')


        threading.Thread(
            target=self.run_leader,
            daemon=True
        ).start()


    def became_follower(self):
        self._isleader = False
        self._votes = 0


        print(f' | {self.get_current_time()} | Process {self._id} virou SEGUIDOR')


        threading.Thread(
            target=self.run_follower,
            daemon=True
        ).start()
    


    def run_leader(self):
        start = time.time()


        while self._running:
            self._count_n_process = 0

            ns = Pyro5.api.locate_ns(host="localhost", port=9095)

            processos = ns.list()

            # print('lenght: ', len(processos), processos)
                
            for nome, uri in processos.items():
                # print('nome: ', nome)
        
                # if i == self._id:
                #     continue
                        
                if nome == "lider":
                    continue

                if nome == "Pyro.NameServer":
                    continue

                if not nome.startswith("process"):
                    continue

                if nome == f"process{self._id}":
                    continue

                try:
                    process_proxy = Pyro5.api.Proxy(uri)
                    process_proxy.heartbeat(self._id)
                    print(f'enviado ao {nome}')
                except:
                    print('não encontrado')
                    pass


            time.sleep(0.400)


    def run_follower(self):
        while self._running:
            elapsed = time.time() - self._last_heartbeat

            if elapsed > self._time_out:
                self._flag = True


                if self._id_last_leader is not None:
                    print(f'\n| {self.get_current_time()} | PROCESS {self._id_last_leader} LIDER CAIU \n')
                
                print(f'| {self.get_current_time()} | Process {self._id} virou CANDIDATO ')
                self._votes = 1
                self._voter_for = self._id

                ns = Pyro5.api.locate_ns(host="localhost", port=9095)

                processos = ns.list()

                # print('lenght: ', len(processos), processos)
                    
                for nome, uri in processos.items():
                    if nome == "lider":
                        continue
                    # if i == self._id:
                    #     continue
                            
                    if nome == "lider":
                        continue

                    if nome == "Pyro.NameServer":
                        continue

                    if not nome.startswith("process"):
                        continue

                    if nome == f"process{self._id}":
                        continue

                    try:
                        process_proxy = Pyro5.api.Proxy(uri)

                        if process_proxy.request_vote(self._id) > 0:
                            self._votes += 1
                            print(f' | {self.get_current_time()} |  recebeu voto de {nome}')


                        self._count_n_process += 1
                        
                    except:
                        print('não encontrado')
                        pass


                if self._votes > (self._count_n_process / 2) :
                    self._isleader = True
                    self._votes = 0
                    self._voter_for = None
                    self._term += 1


                    print(f'| {self.get_current_time()} | Process {self._id} VIROU LIDER')
                    self._count_n_process = 0
                    self.became_leader()
                    break


                else:
                    self._votes = 0
                    self._voter_for = None
                    self._last_heartbeat = time.time()
                    self._time_out = random.uniform(0.500, 0.750)


                    print(f' | {self.get_current_time()} | Process {self._id} PERDEU ELEIÇÃO')
                    self._count_n_process = 0


          
n_process = int(input("nº do processo: "))


daemon = Pyro5.api.Daemon(port=9090 + n_process)
objeto_process = Raft(n_process)


uri = daemon.register(objeto_process, objectId=f"process{n_process}")

servidor_nomes = Pyro5.api.locate_ns(host="localhost", port=9095)
servidor_nomes.register(f"process{n_process}", uri)


print('uri: ', uri)


objeto_process.became_follower()


daemon.requestLoop()







