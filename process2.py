import Pyro5.api
import threading

import time, random

@Pyro5.api.expose
class Raft():
    def __init__(self):
        pass

    
    






    

daemon = Pyro5.api.Daemon()

objeto_client = Raft(1)
callback_uri = daemon.register(objeto_client)

thread = threading.Thread(target=daemon.requestLoop)
thread.daemon = True
thread.start()

# uri = Pyro5.api.locate_ns().lookup("servidor")
uri = "PYRO:meu.servidor@localhost:9090"
proxy = Pyro5.api.Proxy(uri)

resposta = proxy.registrar_cliente(callback_uri)
print(resposta)

# proxy.iniciar_cluster(callback_uri)
# print('iniciou')

while True:
    time.sleep(1)