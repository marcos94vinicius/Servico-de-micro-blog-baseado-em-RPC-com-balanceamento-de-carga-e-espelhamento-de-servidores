import xmlrpc.client
from tkinter import *
from datetime import datetime, date, time
from random import randint
import time

s = xmlrpc.client.ServerProxy('http://191.52.64.177:8590')


# funções que estão no servidor
# Follow
def inserir_usuario():
    nome = ("@" + usuario1.get())
    topico = ("#" + topico1.get())
    l = s.followDisparador(nome, topico)


# post
def envia_post():
    nome = ("@" + usuario2.get())
    topico = ("#" + topico2.get())
    texto = (post2.get())
    hj = date.today()
    l = s.inserePostDisparador(nome, topico, str(hj), texto)


# unsubscribe
def deixar_topico():
    nome = ("@" + usuario3.get())
    topico = ("#" + topico3.get())
    l = s.unsubscribeDisparador(nome, topico)


# retrievetime
def recupera_tudo():
    nome = ("@" + usuario4.get())
    par3 = (data4.get())
    variavel = s.retrieveTimeDisparador(nome, par3)
    ##criando a janela que vai conter os posts
    janela2 = Tk()
    janela2.title("Blog de SO - Posts")
    janela2["bg"] = "green"
    janela2.geometry("1366x768")
    ##criando a label que receberá o que deve ser impresso na tela
    lbPrint = Label(janela2, text=variavel)
    lbPrint.place(x=450, y=130)


# retrievetopic
def recupera_topico():
    nome = ("@" + usuario5.get())
    topico = ("#" + topico5.get())
    par3 = (data5.get())
    l = s.retrieveTopicDisparador(nome, par3, topico)
    ##criando a janela que vai conter os posts
    janela3 = Tk()
    janela3.title("Blog de SO - Posts")
    janela3["bg"] = "green"
    janela3.geometry("1366x768")
    ##criando a label que receberá o que deve ser impresso na tela
    lbPrint = Label(janela3, text=l)
    lbPrint.place(x=450, y=130)

# poll
def recupera_intervalo():
	topico = ("#" + topico6.get())
	d1 = (data6.get())
	d2 = (data7.get())
	qtd = s.pollDisparador(topico, d1, d2)
	##criando a janela que vai conter a quantidade de posts
	janela4 = Tk()
	janela4.title("Blog de SO - Quantidade de posts")
	janela4["bg"] = "green"
	janela4.geometry("1366x768")
	##criando a label que receberá o que deve ser impresso na tela
	lbPrint = Label(janela4, text=qtd)
	lbPrint.place(x=450, y=130)


###########################################################

##criação da janela de interface gráfica
janela = Tk()
janela.title("Blog de SO")
janela["bg"] = "black"
janela.geometry("1366x768")

##criando o botão de inserção do usuário em um determinado tópico
bt1 = Button(janela, width=60, text="Ingressar em algum tópico", command=inserir_usuario)
bt1.place(x=450, y=100)
##caixa de texto onde será inserido o @Username
usuario1 = Entry(janela)
usuario1.place(x=495, y=130)
lb1 = Label(janela, text="Nome:")
lb1.place(x=450, y=130)
##caixa de texto onde será inserido o #tópico
topico1 = Entry(janela)
topico1.place(x=720, y=130)
lb1_1 = Label(janela, text="Tópico:")
lb1_1.place(x=670, y=130)

##criando o botão de inserção de um post
bt2 = Button(janela, width=60, text="Fazer um post", command=envia_post)
bt2.place(x=450, y=160)
##caixa de texto onde será inserido o @Username
usuario2 = Entry(janela)
usuario2.place(x=495, y=190)
lb2 = Label(janela, text="Nome")
lb2.place(x=450, y=190)
##caixa de texto onde será inserido o #tópico
topico2 = Entry(janela)
topico2.place(x=720, y=190)
lb1_2 = Label(janela, text="Tópico:")
lb1_2.place(x=670, y=190)
##caixa de texto onde será inserido o post
post2 = Entry(janela)
post2.place(x=925, y=190)
lb1_2_2 = Label(janela, text="Post:")
lb1_2_2.place(x=890, y=190)

##criando o botão de remoção de um usuário de um determinado tópico
bt3 = Button(janela, width=60, text="Deixar de seguir tópico", command=deixar_topico)
bt3.place(x=450, y=220)
##caixa de texto onde será inserido o @Username
usuario3 = Entry(janela)
usuario3.place(x=495, y=250)
lb3 = Label(janela, text="Nome:")
lb3.place(x=450, y=250)
##caixa de texto onde será inserido o #tópico
topico3 = Entry(janela)
topico3.place(x=720, y=250)
lb3_3 = Label(janela, text="Tópico:")
lb3_3.place(x=670, y=250)

##criando o botão de recuperação de todos os posts que um usuário faz parte
bt4 = Button(janela, width=60, text="Recupera todos os posts de todos os tópicos", command=recupera_tudo)
bt4.place(x=450, y=280)
##caixa de texto onde será inserido o @Username
usuario4 = Entry(janela)
usuario4.place(x=495, y=310)
lb4 = Label(janela, text="Nome")
lb4.place(x=450, y=310)
##caixa de texto onde será inserido a data
data4 = Entry(janela)
data4.place(x=820, y=310)
lb4_4 = Label(janela, text="Data(ex: aaaa-mm-dd):")
lb4_4.place(x=670, y=310)

##O usuário recupera todos os posts, apenas do tópico identificado
bt5 = Button(janela, width=60, text="Recupera todos os posts de um determinado tópico", command=recupera_topico)
bt5.place(x=450, y=340)
##caixa de texto onde será inserido o @Username
usuario5 = Entry(janela)
usuario5.place(x=495, y=370)
lb5 = Label(janela, text="Nome:")
lb5.place(x=450, y=370)
##caixa de texto onde será inserido o #tópico
topico5 = Entry(janela)
topico5.place(x=720, y=370)
lb5_5 = Label(janela, text="Tópico:")
lb5_5.place(x=670, y=370)
##caixa de texto onde será inserido a data
data5 = Entry(janela)
data5.place(x=1040, y=370)
lb5 = Label(janela, text="Data(ex: aaaa-mm-dd):")
lb5.place(x=890, y=370)

##criando o botão de recuperação de todos os posts em um intervalo de tempo
bt6 = Button(janela, width=60, text="Recupera todos os posts dentro do intervalo", command=recupera_intervalo)
bt6.place(x=450, y=400)
##caixa de texto onde será inserido o @Tópico
topico6 = Entry(janela)
topico6.place(x=495, y=430)
lb6 = Label(janela, text="Tópico")
lb6.place(x=450, y=430)
##caixa de texto onde será inserido a primeira data
data6 = Entry(janela)
data6.place(x=720, y=430)
lb6_6 = Label(janela, text="Data 1:")
lb6_6.place(x=670, y=430)
##caixa de texto onde será inserido a segunda data
data7 = Entry(janela)
data7.place(x=940, y=430)
lb7 = Label(janela, text="Data 2:")
lb7.place(x=890, y=430)

##método que executa a interface gráfica
janela.mainloop()

##sript para inserir vários posts
'''ini = time.time()

for i in range(1000):

    usuario = ("Pedro" + str(i))
    val = randint(1, 3)

    if (val == 1):
        l = s.inserePost(usuario, "#sod", '2016-09-22', 'blablablablabla')
    elif (val == 2):
        l = s.inserePost(usuario, "#cc", '2016-09-22', 'blablablablabla')
    elif (val == 3):
        l = s.inserePost(usuario, "#cd", '2016-09-22', 'blablablablabla')

fim = time.time()'''

############################################################

'''usuario1 = ("@Pedro1")
l2 = s.follow(usuario1, "#sod")
l2 = s.follow(usuario1, "#cc")
l2 = s.follow(usuario1, "#cd")
hj2 = date.today()

for i in range(1200):
    texto = ("testando a recuperação dos posts" + str(i))
    if(i >= 0 & i <= 399):
        l = s.inserePost(usuario1, "#sod", str(hj2), texto)
    elif(i >= 400 & i <= 799):
        l = s.inserePost(usuario1, "#cc", str(hj2), texto)
    elif(i >= 800 & i <= 1199):
        l = s.inserePost(usuario1, "#cd", str(hj2), texto)

ini = time.time()
variavel369 = s.retrieveTime(usuario1, '2016-09-24')
print(variavel369)
fim = time.time()
print(fim-ini)'''

