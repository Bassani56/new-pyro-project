import Pyro5.api
import time

import queue
import threading

fila_mensagens = queue.Queue()

def envite_message(process_proxy, mensagem):
    return process_proxy.append_logs(mensagem)


def get_proxy():
    servidor_nomes = Pyro5.api.locate_ns(host="localhost", port=9095)
    uri_objetoPyro = servidor_nomes.lookup("lider")
    return Pyro5.api.Proxy(uri_objetoPyro)

# def remove_lider():
#     servidor_nomes = Pyro5.api.locate_ns(host="localhost", port=9095)
#     servidor_nomes.remove("lider")

count = 0

flag = False

def resend_message():
    global fila_mensagens, flag
    process_proxy = get_proxy()

    while True:
        if fila_mensagens.empty():
            time.sleep(1)
            continue

        try:
            process_proxy = get_proxy()
            msg_pendente = fila_mensagens.get()

            # print("Reenviando:", msg_pendente)
            response = envite_message(process_proxy, msg_pendente)
            print('response: ', response, count)

        except Exception as erro:
            # print("Erro ao reenviar:", erro)
            fila_mensagens.put(msg_pendente)
            flag = True
            time.sleep(4.2)
            flag = False

def send_message():
    global fila_mensagens, count, flag
    running = True

    while running:
        mensagem = str(input("escreva sua mensagem: "))
        process_proxy = get_proxy()

        if mensagem == 'exit':
            break
        
        count += 1

        try:
            response = envite_message(process_proxy, f'{count} - {mensagem}')
            print('response: ', response, count)
           
        except Exception as error:
            print(f'Process {count} ERRO: ')
            fila_mensagens.put(f'{count} - {mensagem}')
            print('reenviando mensagens')

def main():
    threading.Thread(target=send_message, ).start()
    threading.Thread(target=resend_message, ).start()

main()


