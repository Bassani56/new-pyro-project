users_ativos = []

def setar_lista(nova_user):
    global users_ativos
    users_ativos.append(nova_user)

def obter_lista():
    return users_ativos