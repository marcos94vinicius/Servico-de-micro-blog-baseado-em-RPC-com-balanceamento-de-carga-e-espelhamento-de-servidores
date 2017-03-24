# -*- coding: utf-8 -*-
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import sqlite3
import datetime
from itertools import cycle	
import xmlrpc.client
import time
from datetime import datetime, date, time

# importação da biblioteca que tem as chamadas de sistemas de verificação da CPU
import psutil

connection = sqlite3.connect('microblog.db') # Conexao do banco de dados
cursor = connection.cursor() # Cursor para executar queries

# Permite varios usuarios
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

# Cria o Elemento Servidor
server = SimpleXMLRPCServer(("localhost", 8591),
                            requestHandler=RequestHandler)
server.register_introspection_functions()


# Classe dos nós da lista circular da memoria cache
class No:
	def __init__(self, query, info):
		self.query = query
		self.info = info

# Controla quais itens devem ser retirados da lista
controla_cache = 0
alteracao_cache = 0

# Cache
lst_cache = []

# conexao com o outro servidor
server_call2 = xmlrpc.client.ServerProxy('http://localhost:8592')



# Cria tabela Topico
try:
	cursor.execute("CREATE TABLE topico (username varchar(50) PRIMARY KEY, sod int, cc int, cd int)")
except:
	cursor.execute("DROP TABLE topico") # dropa a tabela caso ela ja exista
	cursor.execute("CREATE TABLE topico (username varchar(50) PRIMARY KEY, sod int, cc int, cd int)")

# Cria tabela Post
try:
	cursor.execute("CREATE TABLE post (username varchar(50) PRIMARY KEY, sod int, cc int, cd int)")
except:
	cursor.execute("DROP TABLE post") # dropa a tabela caso ela ja exista
	cursor.execute("CREATE TABLE post (id int PRIMARY KEY, time date, username varchar(50), topico varchar(5), texto varchar(100))")


#Sincronizaçao dos servidores
def Sincroniza():
	#posts = ''
	try:
		hj = date.today()
		data = str(hj)
		cursor.execute("SELECT username,topico,time,texto,id FROM post WHERE time>=(?)",(data,)) # procura todos os posts
		results = cursor.fetchall()
		for row in results:
			username=str(row[0])
			topico=str(row[1])
			time = str(row[2])
			texto = str(row[3])
			idpost = int(row[4])
			try:
				#posts += '$' + str(row[0]) + '$' + str(row[1]) + '$' + str(row[2]) + '$' + str(row[3]) + '$' + str(row[4]) + '\n'
				server_call2.insereVarios(username,topico,time,texto,idpost)
			except:
				resp=0
				
	except:
		print("ERRO AO SINCRONIZAR")
		
	#print(posts + "teste")
	#server_call2.insereVarios(posts)
	return 1
	
server.register_function(Sincroniza, 'sincroniza')

#Sincronizaçao dos servidores
def InsereVarios(nome,topico,timestamp,texto,idpost):
	try:
		cursor.execute("INSERT INTO post VALUES (?,?,?,?,?)",(int(idpost),timestamp,nome,topico,texto,))
		connection.commit()
		#idpost = idpost + 1
		return 1
	except:
		#print("ERRO AO INSERIR")
		return 0

server.register_function(InsereVarios, 'insereVarios')

#Usuario segue um Topico
def Follow (nome,topico,controle):
	
	cursor.execute("SELECT * FROM topico WHERE username=(?)", (nome,))
	x =  cursor.fetchone()
	if x == None:
		if (topico == "#sod"):
			cursor.execute("INSERT INTO topico VALUES (?,?,?,?)",(nome,1,0,0,))
		elif (topico == "#cc"):
			cursor.execute("INSERT INTO topico VALUES (?,?,?,?)",(nome,0,1,0,))
		elif (topico == "#cd"):
			cursor.execute("INSERT INTO topico VALUES (?,?,?,?)",(nome,0,0,1,))
		else:
			
			return 1
	else:
		if (topico == "#sod"):
			cursor.execute("UPDATE topico SET sod=1 WHERE username=(?)",(nome,))
		elif (topico == "#cc"):
			cursor.execute("UPDATE topico SET cc=1 WHERE username=(?)",(nome,))
		elif (topico == "#cd"):
			cursor.execute("UPDATE topico SET cd=1 WHERE username=(?)",(nome,))
		else:
			return 2
	
	connection.commit()
	if(controle!=1):
		try:
			resp = server_call2.follow(nome, topico, 1)
		except:
			print("Servidor 2 ausente")
	
	return 0
	
server.register_function(Follow, 'follow')

#Usuario deicha de seguir um topico
def Unsubscribe(nome, topico, controle):

	try:
		if(topico == "#sod"):
			cursor.execute('UPDATE topico SET sod=0 WHERE username=(?)',(nome,)) # seta 0 no campo do topico o qual o usuario parou de seguir
		elif(topico == "#cc"):
			cursor.execute('UPDATE topico SET cc=0 WHERE username=(?)',(nome,)) # seta 0 no campo do topico o qual o usuario parou de seguir
		elif(topico == "#cd"):
			cursor.execute('UPDATE topico SET cd=0 WHERE username=(?)',(nome,)) # seta 0 no campo do topico o qual o usuario parou de seguir
	except:
		print('Falha ao atualizar a tabela topico.')

	connection.commit() # comita alteracoes feita na tabela
	if(controle!=1):
		try:
			resp = server_call2.unsubscribe(nome, topico, 1)
		except:
			print("Servidor 1 ausente")
			
	return 1

server.register_function(Unsubscribe, 'unsubscribe')



# funçao que ira inserir no banco de posts
def InserePost(nome,topico,timestamp,texto,controle,idpost):
	if (topico == "#cc" or topico == "#cd" or topico == "#sod"):
		try:
			cursor.execute("INSERT INTO post VALUES (?,?,?,?,?)",(int(idpost),timestamp,nome,topico,texto,))
			connection.commit()
			global alteracao_cache
			alteracao_cache = 1
			#idpost = idpost + 1
		except:
			print("ERRO AO INSERIR")
			
		if(controle!=1):
			try:
				resp = server_call2.inserePost(nome,topico,timestamp,texto,1,idpost)
			except:
				print("Servidor 2 ausente")
		
		return 1
	else:
		return 0
		
server.register_function(InserePost, 'inserePost')

def RetrieveTime(nome, tempo):
	global alteracao_cache
	try:
		
		posts = ''
		cursor.execute("SELECT * FROM topico WHERE username=(?) AND sod=1", (nome,))
		x =  cursor.fetchone()
		if x != None:
			cursor.execute("SELECT username,topico,texto FROM post WHERE time>=(?) AND topico='#sod'",(tempo,)) # procura todos os posts de um determinado usuario a partir do tempo especificado
			results = cursor.fetchall()
			for row in results:
				posts += 'Username:' + row[0] + ' topico:' + row[1] + ' postou: ' + row[2] + '\n'
		
		cursor.execute("SELECT * FROM topico WHERE username=(?) AND cc=1", (nome,))
		x =  cursor.fetchone()
		if x != None:
			cursor.execute("SELECT username,topico,texto FROM post WHERE time>=(?) AND topico='#cc'",(tempo,)) # procura todos os posts de um determinado usuario a partir do tempo especificado
			results = cursor.fetchall()
			for row in results:
				posts += 'Username:' + row[0] + ' topico:' + row[1] + ' postou: ' + row[2] + '\n'
		
		cursor.execute("SELECT * FROM topico WHERE username=(?) AND cd=1", (nome,))
		x =  cursor.fetchone()
		if x != None:
			cursor.execute("SELECT username,topico,texto FROM post WHERE time>=(?) AND topico='#cd'",(tempo,)) # procura todos os posts de um determinado usuario a partir do tempo especificado
			results = cursor.fetchall()
			for row in results:
				posts += 'Username:' + row[0] + ' topico:' + row[1] + ' postou: ' + row[2] + '\n'
		#except:
		#	posts = 'Falha ao executar select query.'
	except:
		posts = "Erro: USERNAME inexistente ou DATA escrita de forma errada"
		
	alteracao_cache = 0
	return posts # retorna posts encontrados

server.register_function(RetrieveTime, 'retrieveTime')

def RetrieveTopic(nome, tempo, topico):
	global alteracao_cache
	global controla_cache
	posts = ''
	cache_result = 'n'

	if (topico == "#sod"):
		query = "SELECT * FROM topico WHERE username= " + nome + " AND sod=1"
		if(alteracao_cache == 0):
			cache_result = Cache(query)
		else:
			lst_cache.clear()
			controla_cache = 0
			cache_result = 'n'
		if(cache_result == 'n'):
			cursor.execute("SELECT * FROM topico WHERE username=(?) AND sod=1",(nome,))
			x =  cursor.fetchone()
		else:
			results = cache_result
	elif (topico == "#cc"):
		query = "SELECT * FROM topico WHERE username= " + nome + " AND cc=1"
		if(alteracao_cache == 0):
			cache_result = Cache(query)
		else:	
			lst_cache.clear()
			controla_cache = 0
			cache_result = 'n'
		if(cache_result == 'n'):
			cursor.execute("SELECT * FROM topico WHERE username=(?) AND cc=1",(nome,))
			x =  cursor.fetchone()
		else:
			results = cache_result
	elif (topico == "#cd"):
		query = "SELECT * FROM topico WHERE username= " + nome + " AND cd=1"
		if(alteracao_cache == 0):
			cache_result = Cache(query)
		else:
			lst_cache.clear()
			controla_cache = 0
			cache_result = 'n'
		if(cache_result == 'n'):
			cursor.execute("SELECT * FROM topico WHERE username=(?) AND cd=1",(nome,))
			x =  cursor.fetchone()
		else:
			results = cache_result
	else:
		return "Erro: Topico digitado nao existente"
	
	try:
		if x != None:
			cursor.execute("SELECT username,topico,texto FROM post WHERE time>(?) AND topico=(?)",(tempo,topico,)) # procura todos os posts de um determinado usuario e topico a partir do tempo especificado
			results = cursor.fetchall()
			for row in results:
				posts += 'Username:' + row[0] + ' topico:' + row[1] + ' postou: ' + row[2] + '\n'
	except:
		posts = 'Erro: NOME inexistente ou DATA digitada de maneira errada'

	if(controla_cache < 10):
		controla_cache = controla_cache + 1
		no_cache = No(query, posts)
		lst_cache.append(no_cache)
	else:
		del(lst_cache[0])
		no_cache = No(query, posts)
		lst_cache.append(no_cache)

	alteracao_cache = 0
		
	return posts # retorna posts encontrados

server.register_function(RetrieveTopic, 'retrieveTopic')

# Recupera a quantidade de posts em um intervalo entre duas datas
def Poll(topico, data1, data2):
	global alteracao_cache
	global controla_cache

	try:
		query = "SELECT COUNT(*) FROM post WHERE time>= " + data1 + " AND time<= " + data2
		if(alteracao_cache == 0):
			cache_result = Cache(query)
		else:
			lst_cache.clear()
			controla_cache = 0
			cache_result = 'n'
		if(cache_result == 'n'):
			cursor.execute("SELECT COUNT(*) FROM post WHERE time>=(?) AND time<=(?)", (data1,data2,))
			results = cursor.fetchall()
			if(controla_cache < 10):
				controla_cache = controla_cache + 1
				no_cache = No(query, results)
				lst_cache.append(no_cache)
			else:
				del(lst_cache[0])
				no_cache = No(query, posts)
				lst_cache.append(no_cache)

		else:
			results = cache_result
	except:
		results = 0
	
	alteracao_cache = 0
	return results

server.register_function(Poll, 'poll')

#Funçao para pegar maior idpost
def Idmax():
	retorno = 0
	try:
		cursor.execute("SELECT max(id) FROM post")
		x =  cursor.fetchone()
		retorno = int(x[0])
	except:
		print("ERRO AO PEGAR MAIOR ID")
		
	return retorno
	
server.register_function(Idmax, 'idmax')	

# Função que checa se informação está na cache 
def Cache(query):
	in_cache = 0 
	for item in lst_cache:
		if item.query == query:
			in_cache = 1
			print('Em cache')
			result = item.info
	if(in_cache == 1):
		print(result)
	else:
		result = 'n'

	return result

server.register_function(Cache, 'cache')

# Função que retorna um custo baseado na CPU e RAM
def Custo():

	#Chamada de systema que retorna um float representando em "%" o uso da CPU
	cpu = psutil.cpu_percent(interval=0.1, percpu=False)
	#Chamada de sistema que retorna a porcentagem de uso da RAM
	ram = psutil.phymem_usage().percent

	#Cálculo de uso da CPU baseada na porcentagem usada de CPU mais a porcentagem usada de RAM
	c = ((cpu*0.3)+(ram*0.7))

	return(c)

server.register_function(Custo, 'custo')

server.serve_forever() # faz a parte Servidor rodar em loop e funcionar ate o fim da execucao do programase
