from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import datetime
from random import randint

# Permite varios usuarios
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

# Cria o Elemento Servidor
server = SimpleXMLRPCServer(("localhost", 8002),
                            requestHandler=RequestHandler)

server_call = xmlrpc.client.ServerProxy('http://localhost:8591')
server_call2 = xmlrpc.client.ServerProxy('http://localhost:8592')

server_vivo = 1
server2_vivo = 1

# Id para os posts 
try:
	idpostserver = server_call.idmax()
except:
	idpostserver = server_call2.idmax()

idpost = idpostserver + 1

#Usuario segue um Topico
def FollowDisparador(nome, topico):
	global server_vivo
	global server2_vivo
	teste = Sincroniza()
	try:
		carga = server_call.custo()
	except:
		carga=0
		server_vivo=0
	try:
		carga2 = server_call2.custo()
	except:
		carga2=0
		serve2_vivo=0

	if(carga != 0 and carga2 != 0):

		if carga <= carga2:
			try:
				resp = server_call.follow(nome, topico, 0)
			except:
				resp = server_call2.follow(nome, topico, 0)
		else:
			try:
				resp = server_call2.follow(nome, topico, 0)
			except:
				resp = server_call.follow(nome, topico, 0)
	else:
		if(carga == 0):
			resp = server_call2.follow(nome, topico, 0)
		if(carga2 == 0):
			resp = server_call.follow(nome, topico, 0)		

	return 0
	
server.register_function(FollowDisparador, 'followDisparador')

def InserePostDisparador(nome,topico,timestamp,texto):
	global idpost
	global server_vivo
	global server2_vivo
	teste = Sincroniza()
	try:
		carga = server_call.custo()
	except:
		carga=0
		server_vivo=0
	try:
		carga2 = server_call2.custo()
	except:
		carga2=0
		server2_vivo=0

	if(carga != 0 and carga2 != 0):

		if carga <= carga2:
			try:
				resp = server_call.inserePost(nome,topico,timestamp,texto,0,idpost)
			except:
				resp = server_call2.inserePost(nome,topico,timestamp,texto,0,idpost)
		else:
			try:
				resp = server_call2.inserePost(nome,topico,timestamp,texto,0,idpost)
			except:
				resp = server_call.inserePost(nome,topico,timestamp,texto,0,idpost)
	else:
		if(carga == 0):
			resp = server_call2.inserePost(nome,topico,timestamp,texto,0,idpost)
		if(carga2 == 0):
			resp = server_call.inserePost(nome,topico,timestamp,texto,0,idpost)		
	
	idpost = idpost + 1
	return 0

server.register_function(InserePostDisparador, 'inserePostDisparador')

def UnsubscribeDisparador(nome, topico):
	global server_vivo
	global server2_vivo
	teste = Sincroniza()
	try:
		carga = server_call.custo()
	except:
		carga=0
		server_vivo=0
	try:
		carga2 = server_call2.custo()
	except:
		carga2=0
		server2_vivo=0

	if(carga != 0 and carga2 != 0):

		if carga <= carga2:
			try:
				resp = server_call.unsubscribe(nome, topico, 0)
			except:
				resp = server_call2.unsubscribe(nome, topico, 0)
		else:
			try:
				resp = server_call2.unsubscribe(nome, topico, 0)
			except:
				resp = server_call.unsubscribe(nome, topico, 0)
	else:
		if(carga == 0):
			resp = server_call2.unsubscribe(nome, topico, 0)
		if(carga2 == 0):
			resp = server_call.unsubscribe(nome, topico, 0)		

	return 0

server.register_function(UnsubscribeDisparador, 'unsubscribeDisparador')

def RetrieveTimeDisparador(nome, tempo):
	global server_vivo
	global server2_vivo
	teste = Sincroniza()
	try:
		carga = server_call.custo()
	except:
		carga=0
		server_vivo=0
	try:
		carga2 = server_call2.custo()
	except:
		carga2=0
		server2_vivo=0

	if(carga != 0 and carga2 != 0):

		if carga <= carga2:
			try:
				resp = server_call.retrieveTime(nome, tempo)
			except:
				resp = server_call2.retrieveTime(nome, tempo)
		else:
			try:
				resp = server_call2.retrieveTime(nome, tempo)
			except:
				resp = server_call.retrieveTime(nome, tempo)
	else:
		if(carga == 0):
			resp = server_call2.retrieveTime(nome, tempo)
		if(carga2 == 0):
			resp = server_call.retrieveTime(nome, tempo)	

	return resp

server.register_function(RetrieveTimeDisparador, 'retrieveTimeDisparador')

def RetrieveTopicDisparador(nome, tempo, topico):
	global server_vivo
	global server2_vivo
	teste = Sincroniza()
	try:
		carga = server_call.custo()
	except:
		carga=0
		server_vivo=0
	try:
		carga2 = server_call2.custo()
	except:
		carga2=0
		server2_vivo=0

	if(carga != 0 and carga2 != 0):

		if carga <= carga2:
			try:
				resp = server_call.retrieveTopic(nome, tempo, topico)
			except:
				resp = server_call2.retrieveTopic(nome, tempo, topico)
		else:
			try:
				resp = server_call2.retrieveTopic(nome, tempo, topico)
			except:
				resp = server_call.retrieveTopic(nome, tempo, topico)
	else:
		if(carga == 0):
			resp = server_call2.retrieveTopic(nome, tempo, topico)
		if(carga2 == 0):
			resp = server_call.retrieveTopic(nome, tempo, topico)		

	return resp

server.register_function(RetrieveTopicDisparador, 'retrieveTopicDisparador')

def PollDisparador(topico, data1, data2):
	global server_vivo
	global server2_vivo
	teste = Sincroniza()
	try:
		carga = server_call.custo()
	except:
		carga=0
		server_vivo=0
	try:
		carga2 = server_call2.custo()
	except:
		carga2=0
		server2_vivo=0
		
	if(carga != 0 and carga2 != 0):

		if carga <= carga2:
			try:
				resp = server_call.poll(topico, data1, data2)
			except:
				resp = server_call2.poll(topico, data1, data2)
		else:
			try:
				resp = server_call2.poll(topico, data1, data2)
			except:
				resp = server_call.poll(topico, data1, data2)
	else:
		if(carga == 0):
			resp = server_call2.poll(topico, data1, data2)
		if(carga2 == 0):
			resp = server_call.poll(topico, data1, data2)

	return resp

server.register_function(PollDisparador, 'pollDisparador')

def Sincroniza():
	global server_vivo
	global server2_vivo
	if(server_vivo == 0):
		try:
			aux = server_call2.sincroniza()
			server_vivo = 1
		except:
			server_vivo = 0
	
	if(server2_vivo == 0):
		try:
			aux = server_call.sincroniza()
			server2_vivo = 1
		except:
			server2_vivo = 0
	return 1
	
server.serve_forever() # faz a parte Servidor rodar em loop e funcionar ate o fim da execucao do programa
