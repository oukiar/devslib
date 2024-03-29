'''
State of the art in "cloud storage" proof of concept technology, pythonic and object oriented

Solucion de almacenamiento de informacion basado en modelo de objetos con 
cloud on/offline automatico y sync basado en versiones con solucion automatica de colisiones (duplicacion)

Se cuenta con un esquema distribuido de la base de datos en relacion a los users y devices


ESTE MODULO SIEMPRE TIENE 2 RETORNOS, UNO LOCAL Y OTRO NETWORK
'''


'''
29/oct/2018 Oscar Alcantara Tapia

Nueva documentacion:

Database cloud
Channels for realtime communication
Distributed user management
(Optional) Server Minners


Nodos

Los nodos son las computadoras con las que se tiene enlace, que tipicamente
son las computadoras donde hemos iniciado sesion y las de nuestros contactos.

La informacion que describe como se interrelacionan los nodos es almacenada
en la base de datos local, esto con el fin de crear una especie de base de
datos distribuida tipo user-related-data.



Inicializacion de comunicacion

Existen 2 modos de iniciar el enlace hacia los nodos, el primero es usando 
un servidor con ip publica fija, que facilitara el proceso de inicio de
sincorizacion sin intervencion de usuario

La segunda forma es inicializar la libreria manualmente con el primer nodo, el
cual debe ser la computadora con la que se quiere tener comunicacion en el
formato de IP:PUERTO



Login

El login podra ser ejecutado entre los nodos del usuario o algun nodo de
sus conexiones

Almacen de datos global

Estos datos son almacenados sin necesidad de especificar un usuario, esto
es util para informacion accesible para todos, por ejemplo un blockchain o
un catalogo de productos en un sistema colectivo de inventarios, etc.


Almacen de datos por usuario


Conexiones de usuarios


Replicacion y seguridad




'''

import json
import uuid
import datetime
import time


#import requests

try:
    from kivy.clock import Clock
    from functools import partial

except:
    Clock = None

#disabled, schema must be created in runtime with creation of vars
#from tables import schema

try:
    import psycopg2
    from psycopg2.extensions import AsIs

except:
	psycopg2 = None

#session token
session_token = None
#server_ip = "104.236.181.245"
#server_ip = "162.243.152.20" #orgboat server ip

server_ip = "128.199.49.102" #by default on localhost

servers = [] #list of server minners

server_port = 11235 #fibonacci sequencie port number by default ... all the minners must use the same server_port
user = None
net = None

engine = "sqlite3"

#nodos que se tienen registrados
nodes = {}  #es diccionario para poder acceder a los nodos en base al node_id


#-----------------
#MODULE FUNCTIONS

#use sqlite for local storage
import sqlite3

import os

'''
try:
    import pymysql
except:
    pymysql = None
'''

is_server = False

cnx = None
tables = []
autocommit = True

channels = {} #every channel has a name and the value of the callback


sync_callbacks = {}

write_callbacks = {}

save_callbacks = {}

delete_callbacks = {}

#callback for devices found on local network
callback_found_device = None

callback_list_channels = None
callback_list_channel_devices = None

callback_signup = None
callback_login = None
callback_sync = None
callback_events = None #para eventos cloud


callback_registration = None #callback llamado en respuesta de mensaje de registro en red


'''
curl ifconfig.me
curl icanhazip.com
curl ipecho.net
'''


class Date():
    def __init__(self, dateparam=None):
        
        if dateparam == None:
            self.date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
            return self.date


class Time():
    def __init__(self, timeparam=None):
        
        if timeparam == None:
            self.time = datetime.datetime.fromtimestamp(time.time()).strftime('%H:%M:%S')
            return self.time
            
class Datetime():
    def __init__(self, datetimeparam=None):
        
        self.datetime = None
        
        if datetimeparam == None:
            self.datetime = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            #return self.datetime

def initialized():
    global cnx
    return cnx

#def init(dbname='database.db', server=None, serverport=None, local_port=None):
def init(**kwargs):
    '''
    Local database initialization, most used on devices ... OUTDATED
    '''
    global cnx
    global tables
    global net
    global server_ip
    global server_port
    global engine
    
    engine = kwargs.get("engine", "sqlite3")
        
    if engine == "postgres":
        
        username = kwargs.get("username")
        password = kwargs.get("password")
        db = kwargs.get("db", username)
        server = kwargs.get("server", "127.0.0.1")
        port = kwargs.get("port", "5432")

        cnx = psycopg2.connect("host=%s port=%s user=%s password=%s dbname=%s", (server, port, username, password, db) )
        
    
    else:
        
        dbname = kwargs.get("database", 'database.db')
        #server_ip = kwargs.get("server", None) #none if cloud works only on local mode
        server_port = kwargs.get("serverport", server_port)
        #local_port = kwargs.get("localport", server_port)
        
        #conexion sqlite para base de datos local
        cnx = sqlite3.connect(dbname)
        
        print(cnx)    
        
        '''
        #check if users table exists
        cursor = cnx.cursor()
        #cursor.execute( "SELECT name FROM sqlite_master WHERE type='table' AND name='users'" )
        cursor.execute( "SELECT name FROM sqlite_master WHERE type='table'" )
        
        tables = cursor.fetchall()
        '''
        
        query = Query(className="sqlite_master")
        query.equalTo("type", "table")
        for i in query.find():
            tables.append(i.name)
        
        print("Database schema loaded: " + json.dumps(tables) )
        
        
        #print("Creating network")

        #network
        net = Network()  
        net.create_connection(receiver, server_port)
        
        
        '''
        #resolver ip publica
         
        r = requests.get(r'http://jsonip.com')

        public_ip = r.json()['ip']

        print('Your public IP is', public_ip)
        '''

        '''
        #si no somos el servidor
        if not net.has_ip(server_ip):
            add_node(ip=server_ip, port=server_port)
            print("Primer nodo agregado correctamente")
        else:
            print("Running in server mode")
        '''
        
        #inicia timeout de pings
        if Clock:
            Clock.schedule_interval(ping_nodes, 10)
    
def ping_nodes(dt):
    #print("Enviando ping a nodos ... total (%d)" % len(nodes) )
    
    for i in nodes:
        #print("... enviando ping a nodo ", i)
                
        tosend = json.dumps({'msg':'ping', 'data':datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S') })

        net.send((i['ip'], i['port']), tosend)
    
#init_server ya no se usa, todos deben usar la funcion init y si es server, pasar el parametro server_port
def init_server(**kwargs):
    '''
    Server backend is working under sqlite3
    '''
    global cnx
    global tables
    global net
    global server_port
    global is_server
    
    is_server = True
    
    dbname = kwargs.get("database", "database.db")
    port = kwargs.get("port", server_port)
    
        
    '''
    if True:
        
        
        dbIntegrityError = pymysql.IntegrityError
                                      
        cnx = pymysql.connect(host='localhost',
                                 user='root',
                                 password='CrkXhlHeix',
                                 db='orgboat')
                           
        #autocommit feature enabled
        cnx.autocommit(1)
        
        print_debug = True
        #print_debug = False
        
        #fetch all tables on the database        
        cursor = cnx.cursor()
        cursor.execute("SHOW TABLES")

        result_tables = cursor.fetchall()
        
            
        
        #fetch all fields on all tables
        for i in result_tables:
            tables.append(i[0])
            
            
        if print_debug:
            print("Found %s tables" % str(len(tables)) )
            print("Database schema loaded: " + json.dumps(tables) )
            
                
        #network
        net = Network()  
        net.create_connection(receiver, port) #dispatcher, server_port
            
        print("Cloud server init success")
            
    else:
        print("Error initializing server")
    '''
    
    #nuevo backend en el serverm solo usando sqlite3
    cnx = sqlite3.connect(dbname)
    
    print(cnx)    
    
    '''
    #check if users table exists
    cursor = cnx.cursor()
    #cursor.execute( "SELECT name FROM sqlite_master WHERE type='table' AND name='users'" )
    cursor.execute( "SELECT name FROM sqlite_master WHERE type='table'" )
    
    tables = cursor.fetchall()
    '''
    
    query = Query(className="sqlite_master")
    query.equalTo("type", "table")
    for i in query.find():
        tables.append(i.name)
    
    print("Database schema loaded: " + json.dumps(tables) )
    
    
    print("Creating network")


def register_node(**kwargs):
    global callback_registration
    '''
    Funcion encargada de realizar el registro en la red, se obtiene un
    ID que representa nuestro nodo en la red, dicho ID debe ser almacenado
    para futuras conexiones local y remotas.
    '''
    
    print("Iniciando registro en red distribuida")
    
    
    node_id = kwargs.get("node_id", None)
    callback_registration = kwargs.get("callback_registration", None)
    
    data = {"node_id":node_id, "pubkey":None}
    
    #enviar datos de registro
    tosend = json.dumps({'msg':'register_node', 'data':data})
    net.send((server_ip, server_port), tosend)
    
    
    
       
def add_node(ip, port):
    global nodes
    global net
    
    port = int(port)
    
    nodes.append({'ip':ip, 'port':port})
    
    if net == None:
        
        #network
        net = Network()  
        net.create_connection(receiver, port)
    
        
def bind_events(function_callback):
    global callback_events
    print("EVENTS CALLBACK TO:", function_callback)
    callback_events = function_callback
    
        
def create_channel(channel_name, **kwargs):
    '''
    Channels creation only for servers
    '''
    
    global channels
    
    callback = kwargs.get('notification') 
    callback_connection = kwargs.get('connection', None) 
    callback_new_client_connected = kwargs.get('new_client_connected', None) 
    callback_disconnect = kwargs.get('disconnect', None) 
    mode = kwargs.get('mode', 'readwrite') 
    
    print("Creating channel " + channel_name)
    
    channel_data = {"callback":callback, #same as notification event
                                "callback_connection":callback_connection, 
                                "callback_new_client_connected":callback_new_client_connected, 
                                "callback_disconnect":callback_disconnect, 
                                "mode":mode,
                                "clients":[]}
                                
    channels[channel_name] = channel_data
                                
                                
def list_channels(callback, search=None):

    global callback_list_channels

    callback_list_channels = callback

    tosend = json.dumps({'msg':'list_channels', 'search':search })

    net.send((server_ip, server_port), tosend)
    
    
def list_channel_devices(callback, channel):

    global callback_list_channel_devices

    callback_list_channel_devices = callback

    tosend = json.dumps({'msg':'list_channel_devices', 'channel':channel })

    net.send((server_ip, server_port), tosend)
    
def connect_channel(channel_name, 
                        **kwargs):
    
    #crear canal en cloud local
    create_channel(channel_name, 
                    **kwargs)
                    
    print("Connecting to channel " + channel_name)
    
    #conectar al servidor en el canal
    data = {'channel_name':channel_name, 'mode':kwargs.get('mode', 'readwrite')}

    tosend = json.dumps({'msg':'connect_channel', 'data':data })

    net.send((server_ip, server_port), tosend)
    
    
def write_channel(channel_name, data, write_callback=None):
    
    global write_callbacks
    
    #enviar estos datos al servidor
    
    #request id
    request_id = str(uuid.uuid4())

    tosend = json.dumps({'msg':'write_channel', 'channel_name':channel_name, 'data':data, 'request_id':request_id })

    #net.cb_login = kwargs.get("callback")
    net.send((server_ip, server_port), tosend)
    
    write_callbacks[request_id] = write_callback
    
    return request_id

def create(**kwargs):
    '''
    Create using their objectId

    If classname does not exist, it is created as a table on the database
    '''
    className = kwargs.get("className")
    objectId = kwargs.get("objectId", "")
    
    if objectId != None:
        '''
        Create for and existing register on the database
        '''
        ngvar = NGVar(className=className, objectId=objectId)
    else:
        ngvar = NGVar()
        ngvar.className = className
        
        
    return ngvar

def login(**kwargs):
    '''
    2020: Use login of postgresql as user connected to the dbms
    '''
    global cnx
    global tables
    
    print("Iniciando sesion: " + kwargs["username"])


    username = kwargs.get("username")
    password = kwargs.get("password")
    db = kwargs.get("db")
    server = kwargs.get("server", "127.0.0.1")
    port = kwargs.get("port", "5432")

    try:
        cnx = psycopg2.connect("host=%s port=%s user=%s password=%s dbname=%s" % (server, port, username, password, db) )
        
        
        #obtener la lista de tablas
        
        query = Query(className="pg_catalog.pg_tables")
        query.notEqualTo("schemaname", "pg_catalog")
        query.notEqualTo("schemaname", "information_schema")
        for i in query.find():
            tables.append(i.tablename)
            print(i.tablename)
        
        
        return True
    except:
        return False
    
    return
    #OUTDATED
    '''
    Step 1: Request login on the peers
    Step 2: Request login at localhost
    Step 3: Try to login offline
    '''
    global callback_login
    
    #try to fast login using local user stored in the database
    q = Query(className="users")
    q.equalTo("username", kwargs["username"])
    
    res = q.find()
    
    if len(res) == 0:
        #try to login using email as username
        q = Query(className="users")
        q.equalTo("email", kwargs["username"])
        
        res = q.find()
        
    #if user was found
    if len(res):
        
        #check if the pass is correct
        user = res[0]
        
        print(user)
        
        if user.password == kwargs["password"]:
        
            #login ok, one of the next tasks is try to sync
            
            
            #return the user object
            return user
            
            
        
        print("Bad credentials, password does not match")
        
        return None
        
    else:
        print("Bad credentials, username does not exists")
        
        
        
    
    
    callback_login = kwargs.pop("callback", None)

    tosend = json.dumps({'msg':'login', 'data':kwargs })

    net.send((server_ip, server_port), tosend)
    
    print("Login sent to: " + server_ip + ":" + str(server_port) )
    
def signup(**kwargs):
    '''
    2020: Postgres signup is create user
    '''
    global cnx
    
    username = kwargs.get("username")
    password = kwargs.get("password")
    
    cur = cnx.cursor()
    
    try:
        cur.execute("CREATE USER %s with PASSWORD %s", (AsIs(username), password,))
    
        
    
        return True
    
    except:
        return False
    
    global callback_signup
    
    callback_signup = kwargs.pop("callback", None)

    tosend = json.dumps({'msg':'signup', 'data':kwargs })

    net.send((server_ip, server_port), tosend)

def quit():
    global cnx
    global net
    global tables
    
    net.shutdown_network()
    cnx.close()
    
    cnx = None
    net = None
    tables = []
    
def erase(className):
    '''
    Funcion ecargada de vaciar determinada tabla
    '''
    sql = "delete from " + className
    
    cursor = cnx.cursor()
    
    try:
        if cursor.execute(sql):
            
            if autocommit:
                cnx.commit()
        
    except sqlite3.Error as e:
    
        if 'no such table' in e.args[0]:
            print('sqlite3 Error: No such table')
            print (e.args[0])
        else:
            print('sqlite3 Error: Unknown error')
            print (e.args[0])
                        
        print("Error erasing table: " + sql)
    
def sync_latest(**kwargs):
    print("Syncing latest with: " + kwargs.get('className') )
    
def sync(**kwargs):
    '''
    *****ACTUALIZACION MAYO 31
    
    - 
    '''
    global callback_sync
    
    callback_sync = kwargs.get("callback")
    
    #className = kwargs.get('className', None)
    
    #where = {'clasName':className}
    
    #obtener el ultimo registro de las transacciones
    last_local_transaction = get_max(className='transactions', field='transaction_count')
    #last_local_transaction = get_max(className='transactions', field='transaction_count', where=where)
    
    #get the difference between local last transaction and server live transactons
    data = {'last_transaction':last_local_transaction}
    tosend = json.dumps({'msg':'sync', 'data':data })
    
    print("SENDING:", tosend)
    
    
    #net.cb_sync = sync_callback
    
    net.send((server_ip, server_port), tosend)
    
def sync_callback_store(data_dict, dt):
    
    result = data_dict['result']
    
    print("Total rows: " + str(len(result)) )
    
    
    for i in result:
        print(i)
        ngvar_item = NGVar(className=i['model'])
        ngvar_item.from_values(json.loads(i['object_json']) )
        ngvar_item.save()
        
        
    callback_sync(result)
    
    '''
    #if the table is not on the schema, create one row for the table creation success
    if className not in tables:
        item = result.pop(0)
        
        ngvar_item = NGVar()
        ngvar_item.from_values(item)
        ngvar_item.save()
    
    if len(result):
        
        print(result[0])
        
        #save this sync for local database
        cursor = cnx.cursor()
        
        sql = "insert into " + className + " values(" + create_ss(result[0]) + ")"
        print(sql)
        cursor.executemany(sql, result)
    '''
    
    #call the callback
    #cb_func = sync_callbacks[className]
    #cb_func(result, dt)
    
def get_max(**kwargs):
    '''
    Get the max value of all rows on field
    '''
    className = kwargs.get("className")
    field = kwargs.get("field")
    where = kwargs.get("where", None)

    query = Query(className=className)
    
    if where:
        for i in where:
            query.equalTo(i, where[i])
    
    query.orderby(field, order="DESC")
    res = query.find()
    
    if len(res):
        return getattr(res[0], field)
    
    return 0
    
def get_fieldval(className, objectId, field):
    q = Query(className=className)
    q.equalTo("objectid", objectId)
    
    r = q.find()
    
    if len(r):
        return getattr(r[0], field)
    
def get_sum(className, field, where=""):
    
    sql = "select sum(" + field + ") from " + className
    
    if where != "":
        sql += " where " + where
    
    try:
        cursor = cnx.cursor()
        if cursor.execute(sql):
            #print("SQL: " + sql)
            
            res = cursor.fetchall()
            
            if len(res):
                return res[0][0]
                              
    except sqlite3.Error as e:
    
        if 'no such table' in e.args[0]:
            print('sqlite3 Error: No such table')
            print (e.args[0])
        else:
            print('sqlite3 Error: Unknown error')
            print (e.args[0])
                        
        print("Error SUM: " + sql)
        #print("Values: " + json.dumps(lst_values) )
        
    #fail return None
    return None


def get_count(className, where=""):
    
    sql = "select count(*) from " + className
    
    if where != "":
        sql += " where " + where
    
    try:
        cursor = cnx.cursor()
        if cursor.execute(sql):
            #print("SQL: " + sql)
            
            res = cursor.fetchall()
            
            if len(res):
                return res[0][0]
                              
    except sqlite3.Error as e:
    
        if 'no such table' in e.args[0]:
            print('sqlite3 Error: No such table')
            print (e.args[0])
        else:
            print('sqlite3 Error: Unknown error')
            print (e.args[0])
                        
        print("Error COUNT: " + sql)
        #print("Values: " + json.dumps(lst_values) )
        
    #fail return 0
    return 0
    
def create_ss(arr):
    '''
    Utility function for create the string %S or %? parameters for the databse engine
    '''
    ss = ""
    for i in range(0, len(arr)):
        if ss != "":
            ss += ","
            
        if pymysql != None:
            ss += "%s"
        else:
            ss += "?"
        
        
    return ss

class NGVar:
    '''
    Another try for netget variable
    '''
    def __init__(self, **kwargs):
        global cnx 
        
        self.sql = ""
        self.className = kwargs.get("className")
        self.objectId_key = kwargs.get("objectId_key", None) #this is usefull for compatibility with non standar key fields, is mandatory autoincrement with this mode
        self.saveincloud = True
        
        if cnx == None:
            self.saveincloud = False
        
        self.members_backlist = dir(self)
        
        self.objectId = kwargs.get("objectId", "")
    
        #if objectId is comming on kwargs, initialize with values from database
        if self.objectId != "":
            #get values from the database
            query = Query(className=self.className)
            
            if self.objectId_key == None:
                query.equalTo("objectId", self.objectId)
            else:
                query.equalTo(self.objectId_key, self.objectId)
                
            result = query.find()
            if len(result):
                row = result[0]
                
                for i in dir(row):
                    if i not in self.members_backlist:  
                        setattr(self, i, getattr(row, i) )
                        
                    #print(i)
         
    def update_from_database(self):
        
        #if objectId is comming on kwargs, initialize with values from database
        if self.objectId != "":
            #get values from the database
            query = Query(className=self.className)
            
            if self.objectId_key == None:
                query.equalTo("objectId", self.objectId)
            else:
                query.equalTo(self.objectId_key, self.objectId)
                
            result = query.find()
            if len(result):
                row = result[0]
                
                for i in dir(row):
                    if i not in self.members_backlist:  
                        setattr(self, i, getattr(row, i) )
         
    def get_col_type(self, col):

        if getattr(self, col) == "[AUTO_INCREMENT]":
            tp = "INTEGER PRIMARY KEY" #esto es para autoincrement en sqlite3, posiblemente manejemos este backend primariamente
            has_autoincrement = True
        elif str(type(getattr(self, col) )) in ["<type 'str'>", "<type 'unicode'>", "<class 'str'>"]:
            tp = "TEXT"
        elif str(type(getattr(self, col) )) in ["<type 'int'>", "<class 'int'>"]:
            tp = "INT"
        else:
            tp = "" #esto quizas provoca error al no definir tipo del campo
            
        return tp
    
    def save(self, **kwargs):
        global tables
        '''
        Insertion and Update in the save function, the object must be valid
        
        Be carefull here, because we are compatible with objectId and non standar, the save
            must avoid problems of fields existence
        '''
        
        #print("GUARDANDO IN SAVE", tables)
        
        #sameId = kwargs.get("sameId", False) #util para almacenar objetos que tienen objectId en los values, tales como cuando retorna desde el server
        self.saveincloud = kwargs.pop("saveincloud", False)
        
        if self.className not in tables:
                
            tables.append(self.className)
            
            #create table
            #sql = "CREATE TABLE %s (objectId TEXT PRIMARY KEY NOT NULL" % (self.className)
            
            #print("Recorriendo elementos de self")
            
            right_sql = ""
            has_autoincrement = False
            
            #recorrer con dir los elementos de nuestro objeto, para saber que campos llevara
            for i in dir(self):
                if i not in self.members_backlist and i != "members_backlist":
                    
                    if i == "objectId":
                        continue
                    
                    #print(i, type(getattr(self, i) ) )
                    
                    if getattr(self, i) == "[AUTO_INCREMENT]":
                        tp = "INTEGER PRIMARY KEY" #esto es para autoincrement en sqlite3, posiblemente manejemos este backend primariamente
                        has_autoincrement = True
                    elif str(type(getattr(self, i) )) in ["<type 'str'>", "<type 'unicode'>", "<class 'str'>"]:
                        tp = "TEXT"
                    elif str(type(getattr(self, i) )) in ["<type 'int'>", "<class 'int'>"]:
                        tp = "INT"
                        
                    elif str(type(getattr(self, i) )) == "<class 'devslib.cloud.Date'>":
                        tp = "DATE"
                        
                    elif str(type(getattr(self, i) )) == "<class 'devslib.cloud.Time'>":
                        tp = "TIME"
                        
                    elif str(type(getattr(self, i) )) == "<class 'devslib.cloud.Datetime'>":
                        tp = "TIMESTAMP"
                        
                    else:
                        tp = "" #esto quizas provoca error al no definir tipo del campo
                    
                    if right_sql == "":
                        right_sql = i + " " + tp
                    else:
                        right_sql += ", " + i + " " + tp
            
            '''
            if has_autoincrement or 'objectId' in right_sql:
                sql = "CREATE TABLE %s (" % (self.className) + right_sql + ");"
            else:
                sql = "CREATE TABLE %s (objectId TEXT PRIMARY KEY NOT NULL, " % (self.className)  + right_sql +   ");"
            '''
            
            sql = "CREATE TABLE %s (objectId serial PRIMARY KEY, " % (self.className)  + right_sql +   ");"
            
            
            print(sql)
            
            #crear tabla
            cursor = cnx.cursor()
            
            try:
                cursor.execute(sql)
                cnx.commit()
                    
            except (Exception, psycopg2.DatabaseError) as error:
                print('Error at table creation', error)
        else:
            pass
            #print("Table already exists")
        
        if self.objectId != "":
            return self.real_save(**kwargs)
        else:
            return self.real_insert(**kwargs)
            
            
            
    def real_save(self, **kwargs):
        
        #el update solo funcionara con conexion
        #print("SAVE UPDATE !!!!! ----- CHECKIT ")
        
        #update
        sql = "update " + self.className + " set "
        
        values = ""
        lst_values = []
        
        for i in dir(self):
            if i not in self.members_backlist and i != "members_backlist":
                                    
                #print (str(type(getattr(self, i) )))
                
                '''
                if str(type(getattr(self, i) )) in ["<type 'int'>", "<class 'int'>"]:
                    val = str(getattr(self, i))
                else:
                    try:
                        val = "'" + str(getattr(self, i) ) + "'"
                    except:
                        val = "'" + str(getattr(self, i).encode('utf8') ) + "'"
                '''
                
                lst_values.append(getattr(self, i))
        
                if values == "":
                    values = i + "=?" #+ val
                    #values = i + "=" + val
                else:
                    values += ", " + i + "=?" #+ val
                    #values += ", " + i + "=" + val
        
        sql += values + " where objectId='"+ self.objectId + "'"  
        
        
        try:
            cursor = cnx.cursor()
            cursor.execute(sql, lst_values)
            if autocommit:
                cnx.commit()
            
            return True
            
            if cursor.execute(sql):
                #print("SQL: " + sql)
                #print("Modified rows: " + str(cursor.rowcount))
                
                #si no se pudo actualizar entonces no existe, debe ser creado
                if cursor.rowcount == 0:
                    self.real_insert(**kwargs)
                
                if autocommit:
                    cnx.commit()
                        
                if self.className != 'transactions':
                    
                    if self.saveincloud:
                        
                        #guardar en todos los nodos con los que se tiene conexion
                        for i in nodes:
                            
                            print("Node:", i)
                        
                            print('Sync save to node', self.className)
                            
                            
                            #guardar el callback que sera llamado en respuesta a esta llamada
                            save_callbacks[self.objectId] = kwargs.get('callback', None)
                                
                            #send to the server the event for update in the cloud, only if we have connection
                            tosend = json.dumps({'msg':'update_from_client', 'className':self.className, 'data':self.fix_to_json() })
                            #send to the server
                            net.send((server_ip, server_port), tosend)
                            
                    
                    '''
                    #guardar esta transaccion en la tabla de transacciones
                    t = create(className='transactions')
                    t.model = self.className
                    t.operation = 'update'
                    t.object_json = json.dumps(self.fix_to_json())
                    t.sql = sql
                    t.json_values = json.dumps(lst_values)
                    t.timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                    t.transaction_count = "[AUTO_INCREMENT]"
                    t.save(saveincloud=False)
                    '''
                    
                return True
                                  
        except sqlite3.Error as e:
        
            if 'no such table' in e.args[0]:
                print('sqlite3 Error: No such table')
                print (e.args[0])
            elif 'no such column' in e.args[0]:
                col = e.args[0].split(': ')[1]
                print("Columna no detectada, creando: " + col)
                
                typecol = self.get_col_type(col)
                
                sql = "ALTER TABLE " + self.className + " ADD COLUMN " + col + " " + typecol + ";"
                
                print(sql)
                
                cursor = cnx.cursor()
                
                if cursor.execute(sql):
                    self.real_save(**kwargs)
                else:
                    print("Error creando nueva columna")
                
            else:
                print('sqlite3 Error: Unknown error')
                print(sql)
                print (e.args[0])
                            
            #print("Error updating-saving: " + sql)
            #print("Values: " + json.dumps(lst_values) )
        
        return False
        
    def real_insert(self, **kwargs):
        global is_server
        
        #create object unique ID ... talvez esto deba ser comentado para postgres backend
        if self.objectId == '':
            self.objectId = str(uuid.uuid4())
            
        
        #-------- INSERCION 
        
        values = ""
        lst_values = []
        
        has_autoincrement = False
        sqlfields = ""
        
        #esto estaba provocando duplicacion de registros, no es necesario pues ya se extrae en la funcion save y guarda en self.saveincloud
        #self.saveincloud = kwargs.pop("saveincloud", True)
        
        for i in dir(self):
            if i not in self.members_backlist and i != "members_backlist":
            
                #esto debe ser rehabilitado para postgress backend
                #if i == "objectId":
                #    continue
                    
                if getattr(self, i) == "[AUTO_INCREMENT]":
                    has_autoincrement = True
                    continue
                
                if sqlfields == "":
                    sqlfields += " " + i #HERE: avoid the sqlinjection
                else:
                    sqlfields += ", " + i #HERE: avoid the sqlinjection
                
                #print ("TYPE", str(type(getattr(self, i) )))
                
                
                if values == "":
                    values += "?" 
                else:
                    values += ", ?"
                     
                #lst_values.append(getattr(self, i))
              
                
                if str(type(getattr(self, i) )) in ["<type 'int'>", "<class 'int'>", "<class 'devslib.cloud.Date'>", "<class 'devslib.cloud.Time'>"]:
                    lst_values.append( getattr(self, i) )
                    
                elif str(type(getattr(self, i) )) == "<class 'devslib.cloud.Datetime'>":
                    lst_values.append( getattr(self, i).datetime )
                    
                else:
                    lst_values.append( getattr(self, i) )
                
                
                '''
                if values != "":
                    values += ", "
                
                if str(type(getattr(self, i) )) in ["<type 'int'>", "<class 'int'>", "<class 'devslib.cloud.Date'>", "<class 'devslib.cloud.Time'>"]:
                    values += str(getattr(self, i))
                    
                elif str(type(getattr(self, i) )) == "<class 'devslib.cloud.Datetime'>":
                    values += "'" + str(getattr(self, i).datetime) + "'"
                    
                else:
                    values += "'" + str(getattr(self, i) ) + "'"
                '''
                
        '''
        if has_autoincrement or 'objectId' in sqlfields:
            sql = "insert into " + self.className + "(" + sqlfields + ") values(" + values + ");" 
        else:
            sql = "insert into " + self.className + "(objectId, " + sqlfields + ") values('"+ self.objectId + "', " + values + ");" 
        '''
        
        sql = "insert into " + self.className + "(" + sqlfields + ") values(" + values + ");" 
        
        
        try:
            cursor = cnx.cursor()
            
            #print("SQL: " + sql)
            
            cursor.execute(sql, lst_values)
            
            if autocommit:
                cnx.commit()
            
            return cursor.lastrowid
            
            if cursor.execute(sql):
                #if autocommit:
                cnx.commit()
                    
                    
                if self.saveincloud:
                    
                    #enviar datos a todos los nodos
                    for  i in nodes:
                        print("Node:", i)
                        print('Insert Sync save to node', self.className)
                        
                        
                        #guardar el callback que sera llamado en respuesta a esta llamada
                        save_callbacks[self.objectId] = kwargs.get('callback', None)
                            
                        #send to the server the event for update in the cloud, only if we have connection
                        tosend = json.dumps({'msg':'update_from_client', 'className':self.className, 'data':self.fix_to_json() })
                        
                        #send this message to the peers
                        for i in nodes:
                            print("SENDING TO NODE", i)
                            
                            #send to the node
                            net.send((i["ip"], i["port"]), tosend)
                        
                    
                return cursor.lastrowid
                
        except sqlite3.Error as e:
        
            if 'no such table' in e.args[0]:
                print('sqlite3 Error: No such table')
                print (e.args[0])
            elif 'no such column' in e.args[0] or 'no column named' in e.args[0]:
                try:
                    col = e.args[0].split(': ')[1]
                except:
                    col = e.args[0].split('no column named ')[1]
                    
                print("Columna no detectada, creando: " + col)
                
                typecol = self.get_col_type(col)
                
                sql = "ALTER TABLE " + self.className + " ADD COLUMN " + col + " " + typecol + ";"
                
                print(sql)
                
                cursor = cnx.cursor()
                
                if cursor.execute(sql):
                    self.real_insert(**kwargs)
                else:
                    print("Error creando nueva columna")
            else:
                print('sqlite3 Error: Unknown error real_insert')
                print (sql, lst_values)
                print (e.args[0])
            
            #print("Error saving: " + sql)
            #input()
        
        return False

    def delete(self, **kwargs):
        '''
        By default delete the row ... 
        '''
        sql = "delete from " + self.className + " where objectId='" + self.objectId + "'"
        print("Deleting: " + sql)
        
        self.saveincloud = kwargs.pop("saveincloud", False)
        
        cursor = cnx.cursor()
        if cursor.execute(sql):
            #print("SQL: " + sql)
            if autocommit:
                cnx.commit()
                
            if self.saveincloud:
                if is_server == False and server_ip != None:
                    #sync this deletion with the server
                    
                    #guardar el callback que sera llamado en respuesta a esta llamada
                    delete_callbacks[self.objectId] = kwargs.get('callback', None)
                        
                    #send to the server the event for update in the cloud, only if we have connection
                    tosend = json.dumps({'msg':'delete_from_client', 'className':self.className, 'data':self.fix_to_json() })
                    #send to the server
                    net.send((server_ip, server_port), tosend)
                '''
                #save this transaction locally
                if self.className != 'transactions':
                    #guardar esta transaccion en la tabla de transacciones
                    t = create(className='transactions')
                    t.model = self.className
                    t.operation = 'delete'
                    t.object_json = json.dumps(self.fix_to_json())
                    t.sql = sql
                    t.json_values = None
                    t.timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                    t.transaction_count = "[AUTO_INCREMENT]"
                    t.save()
                '''
            return True
        
        return False
        
    def fix_to_json(self):
        
        values = {}
        
        for i in dir(self):
            if i not in self.members_backlist and i != "members_backlist":
                
                try:
                    values[i] = getattr(self, i).isoformat()
                except:
                    values[i] = getattr(self, i)
        
        return values
        
    def from_values(self, values):
        for i in values:
            #print(i)
            setattr(self, i, values[i])
        

class Query:
    '''
    This is the class that will let you get data from the cloud
    '''
    def __init__(self, **kwargs):
        self.className = kwargs.get('className')
        self.select = kwargs.get('select', "*")
        
        self.conditions = []
        
        '''
        if self.className:
            self.sql = "select " + self.select + " from " + self.className
            self.params = []
            self.conditions = ""
            self.order = ""
            self.group = ""
            self.maxlimit = ""
        '''

    def generate_sql(self):
        
        self.sql = "select " + self.select + " from " + self.className
        
        #print(self.conditions)
        
        if len(self.conditions):
            self.sql += " where 1=1"
            
            for i in self.conditions:
                #print("I:", i)
                if i["condition"] in ("=", "!=", " LIKE "):
                    self.sql += " AND "  + i["field"] + i["condition"] + "'" + str(i["value"]) + "'" 
                    
                elif i["condition"] in ("=", " ORLIKE "):
                    self.sql += " OR "  + i["field"] + " LIKE " + "'" + str(i["value"]) + "'" 
                    
                elif i["condition"] in ("<", ">"):
                    self.sql += " AND "  + i["field"] + i["condition"] + str(i["value"])
                    
                elif i["condition"] in ("f=", "f<", "f>"):
                    self.sql += " AND "  + i["field"] + i["condition"][1] + i["value"] 
                    
                elif i["condition"] in ("ORDER BY"):
                    self.sql +=  " " + i["condition"] + " " +  i["field"] + " " + str(i["value"]) + " "
                    
                elif i["condition"] in ("GROUP BY"):
                    self.sql +=  " " + i["condition"] + " " +  i["field"]
                    
                elif i["condition"] in ("IN"):
                    self.sql +=  " AND "  + i["field"] + " " + i["condition"] + " (" + str(i["value"]) + ") "
                    
                elif i["condition"] in ("LIMIT", "OFFSET"):
                    self.sql +=  " " + i["condition"] + " " + str(i["value"]) + " "
                    
                elif i["condition"] in ("BETWEEN"):
                    self.sql +=  " AND " + i["field"] + " " + i["condition"] + " '" + str(i["value"][0]) + "' AND '" + str(i["value"][1]) + "' "
                
        print (self.sql)
        return self.sql

    def find(self, **kwargs):
        
        if self.className not in tables and self.className != "pg_catalog.pg_tables" and len(tables) != 0:
            return []
        
        raw = kwargs.get("raw", False)
        customsql = kwargs.get("customsql", False)
        
        if customsql == False:
            self.generate_sql()
        
        cursor = cnx.cursor()
        print( (self.sql) )

        try:
            cursor.execute(self.sql)
        except sqlite3.Error as e:
            
            if 'no such table' in e.args[0]:
                print('sqlite3 Error: No such table')
                print (e.args[0])
            else:
                print('sqlite3 Error: Unknown error')
                print (e.args[0])
        
        #get all results
        res = cursor.fetchall()
        
        #print (res)
        #print cursor.description
                
        results = []
        
        for r in res:
                    
            if raw:
                row = {'className':self.className}
            else:
                row = NGVar(className=self.className)
                
            #FILL THE ROW
            count = 0
            for i in cursor.description:
                
                #current field name
                fieldname = i[0]
                
                #STORE THE ROW
                if raw:
                    #store as simple dict
                    try:
                        row[fieldname] = r[count].isoformat()   #datetime is better as string format
                    except:
                        row[fieldname] = r[count]
                else:
                    #set the atribute for the ngvar
                    setattr(row, fieldname, r[count])
                
                #print(r[count])
                
                count += 1
                
                
                '''
                #print type(r[count])
                
                if str(type(r[count])) in ["<type 'datetime.datetime'>", "<type 'datetime.date'>"]:
                    row[i] = r[count].isoformat()
                else:
                    row[i] = r[count]
                    
                
                print("TYPE: " + str(type(row[i])) )
                print(json.dumps(row[i], encoding='latin1')) 
                '''
                
                    
                    
            results.append( row )
                

        return results

    def sync(self, callback=None):
        print("Sincronizar usando query")
        
        #generar sql
        
        
        #indicar cual es el callback para esta sincronizacion
        
        
        #enviar peticion al server
        



    def equalTo(self, field, value):
        
        self.conditions.append({"field":field, "condition":"=", "value":value})
        '''
        if self.conditions != "":
            self.conditions += " AND "
        
        if pymysql != None:
            self.conditions += "`" + field + "`=%s"
        else:
            self.conditions += field + "=?"
        
        
        #self.conditions += field + "=?"
            
        #self.params.append(field)
        self.params.append(value)
        '''
        
    def notEqualTo(self, field, value):
        
        self.conditions.append({"field":field, "condition":"!=", "value":value})
        
    def like(self, field, value):
        self.conditions.append({"field":field, "condition":" LIKE ", "value":value})
        
    def orlike(self, field, value):
        self.conditions.append({"field":field, "condition":" ORLIKE ", "value":value})
        
    def orderby(self, field, order='ASC'):
        
        self.conditions.append({"field":field, "condition":"ORDER BY", "value":order})
        '''
        if self.order == "":
            self.order = " ORDER BY " + field + " " + order + " "
        else:
            self.order += ", " + field + " " + order + " "
        '''
            
    def groupby(self, field):
        
        self.conditions.append({"field":field, "condition":"GROUP BY", "value":None})
        '''
        if self.group == "":
            self.group = " GROUP BY " + field + " "
        else:
            self.group += ", " + field + " "
        '''
        
    def where(self, where_dict):
        #print('WHERE')
        for i in where_dict:
            #print(i)
            self.equalTo(i, where_dict[i])
            
    def greaterThan(self, field, value):
        
        self.conditions.append({"field":field, "condition":">", "value":value})
        '''
        #if conditions are yet initialized
        if self.conditions != "":
            self.conditions += " AND "
            
        
        if pymysql != None:
            self.conditions += " `" + field + "`>%s"
        else:
            self.conditions += ' ' + field + '>?'
        
        
        #self.conditions += ' ' + field + '>?'
        
        self.params.append(value)  
        '''
        
    def lessThan(self, field, value):
        self.conditions.append({"field":field, "condition":"<", "value":value})
        
    def fieldLessThan(self, field, field2):
        self.conditions.append({"field":field, "condition":"f<", "value":field2})
        
    def between(self, field, field2, field3):
        self.conditions.append({"field":field, "condition":"BETWEEN", "value":(field2, field3)})
        
    def limit(self, n):
        
        self.conditions.append({"field":None, "condition":"LIMIT", "value":str(n)})
        '''
        self.maxlimit = " LIMIT " + str(n)
        '''
        
    def skip(self, n):
        self.conditions.append({"field":None, "condition":"OFFSET", "value":str(n)})
        
    def in_values(self, field, arr):
        
        self.conditions.append({"field":field, "condition":"IN", "value":str(tuple(arr))[1:-1]})
        '''
        #if conditions are yet initialized
        if self.conditions != "":
            self.conditions += " AND "
            
        if pymysql != None:
            self.conditions += ' ' + field + ' in %s'
        else:
            self.conditions += ' ' + field + ' in ?'
        
        self.params.append(arr)
        '''
        
    def subquery(self, field, sq):
        #print("Subquery: ", sq.generate_sql() )
        
        self.conditions.append({"field":field, "condition":"IN", "value":sq.generate_sql()})
        
        '''
        #if conditions are yet initialized
        if self.conditions != "":
            self.conditions += " AND "

        self.conditions += ' ' + field + ' in (' + sq.generate_sql() + ")"
        '''


#cada canal contiene la lista de clientes conectados y el valor asociado
channels = {}


class CloudVar():
    '''
    Variable autosincronizable entre diferentes nodos de la red usando el
    networking p2p
    
    Dado a la naturaleza de las sincronizaciones, este objeto esta enfocado
    a sincronizar variables ya sea string, entero o float
    '''
    
    def __init__(self, **kwargs):
        super(CloudVar, self).__init__(**kwargs)
        
        self.sync_callback = kwargs.get("callback", None)
    
    def link(self, serverip, channel):
        '''
        Establece enlace de una variable usando el concepto de canal de comunicacion
        '''
        global channels
        
        self.server = serverip
        self.channel = channel

        #conectar al servidor en el canal especificado
        if channel not in channels:
            channels[channel] = {"clients":None, "data":None}
            
        #enviar mensaje al servidor de conexion al canal 
        
        
    def sync(self):
        '''
        
        '''
        pass
        
    def unlink(self):
        pass

#--------------------------------------
#NETWORKING STUFF
#all this code is only for low level communications of cloud synchronizations
#

try:
    from network import Network
except:
    from devslib.network import Network

import socket


try:
    import bcrypt
except:
    bcrypt = None
    

def callback_bridge(self, *args, **kwargs):
    callback = kwargs.get("callback")
    callback(**kwargs)
    
def send_ping(dt):
    '''
    Sending ping to the server for maintain the holepunch
    '''
    print("Sending ping")
    
    tosend = json.dumps({'msg':'ping', 'data':socket.gethostname()})
    net.send((server_ip, server_port), tosend)
    
    
import threading

server_dispatcher_lock = threading.Lock()

server_dispatchers = []

def dispatch_from_server():
    server_dispatcher_lock.acquire()
    
    if len(server_dispatchers):
        function_kwargs = server_dispatchers.pop(0)
        print(function_kwargs)
        
        callback = function_kwargs["function"]
        addr = function_kwargs["addr"]
        data_dict = function_kwargs["data_dict"]
        
        callback(addr, data_dict)
    
    server_dispatcher_lock.release()
    
def dispatch_save(addr, data_dict):
    
    #AQUI AGREGAR UN PARAMETRO QUE SIRVA PARA FORZAR LA CREACION DE LA VARIABLE A PESAR DE NO EXISTIR
    #AQUI SE VA A REPARAR EL PROBLEMA DE QUE SE ESTAN DUPLICANDO LOS ELEMENTOS AL SINCRONIZARSE Y NO TENER LA TABLA EN EL SERVER
    print("DISPATCH SAVEE")
    
    updatingvar = create(className=data_dict['className'], objectId=data_dict['data']['objectId'])
    
    updatingvar.from_values(data_dict['data'])
    
    #enviamos false para indicar que no actualize cloud, pues normalmente esto sucede en serverside
    updatingvar.save(saveincloud=False)
    
    #avisamos al cliente que el save fue satisfactorio
    tosend = json.dumps({'msg':'update_from_client_ack', 'objectId':data_dict['data']['objectId']}, encoding='latin1')
    
    net.send(addr, tosend)
    
def dispatch_sync(addr, data_dict):

    data = data_dict['data']
    
    #obtener los registros mayores al ultimo existente en el cliente remoto
    q = Query(className="transactions")
    
    q.greaterThan('transaction_count', int(data['last_transaction']) )
    
    #result = q.find(raw=True, customsql=True)
    result = q.find(raw=True)
    
    tosend = json.dumps({'msg':'sync_ack', 'result':result, "className":'ALL'}, encoding='latin1')
    
    print("RESPONSE_TOSEND:", tosend)
    print("Sync Result: " + str(len(result)) )
    
    net.send(addr, tosend)
        
    
def dispatch_signup(addr, data_dict):
    
    #try to sign up this new user
    print(data_dict['data']['username'])

    q = Query(className="users")
    q.equalTo('username', data_dict['data']['username'])
    result = q.find()

    #check that username does not exist
    if len( result ):
        #user already exists

        tosend = json.dumps({'msg':'signup_ack', 'result':"error", "errormsg":"Signup fail: Already in use"})
        net.send(addr, tosend)
        return
        
    sessiontoken = str(uuid.uuid4())

    #new user initialization
    newuser = create(className="users")
    
    newuser.from_values(data_dict['data'])
    
    newuser.modules = ""
    newuser.groups = ""
    newuser.privileges = ""
    
    '''
    newuser.setval('username', data_dict['data']['username'])
    newuser.setval('password', data_dict['data']['password'])
    newuser.setval('email', data_dict['data']['email'])
    '''
    
    newuser.sessiontoken = sessiontoken

    #it is done in the server, can be syncronously
    if(newuser.save(saveincloud=False) ):
        tosend = json.dumps({'msg':'signup_ack', 'result':"welcome", "newuser":newuser.fix_to_json() })
    else:
        tosend = json.dumps({'msg':'signup_ack', 'result':"error", "errormsg":newuser.error})

    print("Response to login: ", tosend)

    net.send(addr, tosend)
    
    
def dispatch_login(addr, data_dict):
    
    #try to sign up this new user
    print(data_dict['data']['username'])

    q = Query(className="users")
    q.equalTo('username', data_dict['data']['username'])
    result = q.find()
    
    if len( result ) == 0:
        q = Query(className="users")
        q.equalTo('email', data_dict['data']['username'])
        result = q.find()
    

    #check that username exists
    if len( result ):
        '''
        #yes, you can send messages async
        tosend = json.dumps({'msg':'login_ack', 'data':"already_used"})
        net.send(addr, tosend)
        return
        '''
        
        #user = NGVar(values=result[0]["values"])
        user = result[0]
        
        print(result)
        
        print("User received pass: " + data_dict['data']['password'] )
        print("User password hash: " + user.password )
        
        hashed = user.password
        
        #verify that password is correct
        if data_dict['data']['password'] == user.password:            
        #if bcrypt.hashpw( data_dict['data']['password'].encode("latin1"), hashed ) == hashed:
            
            #great, everithing is ok!, starting session
        
            #sessiontoken = os.urandom(32)

            #user.setval('sessiontoken', sessiontoken)

            tosend = json.dumps({'msg':'login_ack', 'result':"welcome", "user":user.fix_to_json()})
            net.send(addr, tosend)
            
            return
            
        else:
            tosend = json.dumps({'msg':'login_ack', 'result':"error", "errormsg":"Passwords does not match"})
            net.send(addr, tosend)        
            return        
    
    #todo: check if the next result.error is correct
    tosend = json.dumps({'msg':'login_ack', 'result':"error", "errormsg":"User does not exists"})
    net.send(addr, tosend)
    return
    

def receiver(data, addr):
    
    '''
    All callbacks must be called with Clock.schedule_once  due to this fuction is not from the main thread
    '''
    
    global channels
    global callback_bridge
    global send_ping
    global callback_found_device
    global nodes
    
    data_dict = json.loads(data)
    
    if 'msg' not in data_dict:
        return

    if data_dict['msg'] == 'ping_ack':        
        print('PING ACK RECEIVED FROM ', addr, data_dict['data'])
        
        #almacenar estos datos que corresponden a nuestra conexion publica en internet, se usara para el holepunch
        
        
        #en este punto ya podemos almacenar cosas en los datos globales del cloud
        
        
        '''
        if addr[0] != net.ngsock.addr[0] and callback_found_device != None:
            Clock.schedule_once(partial(callback_found_device, addr, data_dict['data']), 0)
        '''

    elif data_dict['msg'] == 'ping':
        
        print('PING RECEIVED FROM ', addr, data_dict['data'])
        
        data = {'hostname':socket.gethostname(), 'ip':addr[0], 'port':addr[1]}
        
        tosend = json.dumps({'msg':'ping_ack', 'data':data})
        net.send(addr, tosend)
        '''
        #almacenar
        v = create(className='peers')
        v.ip = addr[0]
        v.port = addr[1]
        v.createdAt = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        
        v.save(saveincloud=False)
        '''
        
    elif data_dict['msg'] == 'signup_ack':
        
        Clock.schedule_once(partial(callback_signup, data_dict), 0)
        
    elif data_dict['msg'] == 'login_ack':
        
        #print('SIGNUP ACK FROM', addr, data_dict['data'])
        Clock.schedule_once(partial(callback_login, data_dict), 0)

    elif data_dict['msg'] == 'sync_ack':
        
        #print('SIGNUP ACK FROM', addr, data_dict['result'])
        #net.cb_login(data_dict)
        Clock.schedule_once(partial(sync_callback_store, data_dict), 0)
        
    elif data_dict['msg'] == 'transmission_ack':
        
        #print('SIGNUP ACK FROM', addr, data_dict['result'])
        #net.cb_login(data_dict)
        Clock.schedule_once(partial(net.transmission_done, addr, data_dict['trans_id']) )

    #------------------
    #ONLY SERVERS ACTIONS
    elif data_dict['msg'] == 'signup':
        
        print('SIGNUP FROM ', addr, data_dict['data'])
                    
        server_dispatcher_lock.acquire()
        
        server_dispatchers.append( {"function":dispatch_signup, "addr":addr, "data_dict":data_dict} )
        
        server_dispatcher_lock.release()
        
        
    elif data_dict['msg'] == 'login':
        
        print('LOGIN FROM ', addr, data_dict['data'])
        
        server_dispatcher_lock.acquire()
        
        server_dispatchers.append( {"function":dispatch_login, "addr":addr, "data_dict":data_dict} )
        
        server_dispatcher_lock.release()
        

    elif data_dict['msg'] == 'sync':
        
        print('SYNC FROM ', addr, data_dict['data'])
        
        server_dispatcher_lock.acquire()
        
        server_dispatchers.append( {"function":dispatch_sync, "addr":addr, "data_dict":data_dict} )
        
        server_dispatcher_lock.release()
        

    
    elif data_dict['msg'] == 'list_channels': 
        
        print('LISTING CHANNELS FROM: ', addr, data_dict['data'])
        
        tosend = json.dumps({'msg':'list_channels_ack', 'channels':channels}, encoding='latin1')
        
        net.send(addr, tosend)
        
    elif data_dict['msg'] == 'list_channel_devices': 
        
        print('LISTING CHANNEL DEVICES FROM: ', addr, data_dict['data'])
        
        channel = data_dict['channel']
        
        tosend = json.dumps({'msg':'list_channel_devices_ack', 'channels':channels[channel]['clients']}, encoding='latin1')
        
        net.send(addr, tosend)
        
    elif data_dict['msg'] == 'list_channels_ack': 
        
        lst_channels = data_dict['channels']
        
        Clock.schedule_once(partial(callback_list_channels, lst_channels), 0)
        
    elif data_dict['msg'] == 'connect_channel': 
        
        print('CHANNEL CONNECTION FROM: ', addr, data_dict['data'])

        data = data_dict['data']
        
        channel_name = data["channel_name"]
        mode = data["mode"]
        
        #if the channel is not in this server, create it
        if channel_name not in channels:
            #create channel without callback
            create_channel(channel_name)
        
        newclient = {'addr':addr, 'mode':mode}
        
        #agregar este cliente a channel para hacer las notificaciones cuando hagan channel_write
        if newclient not in channels[channel_name]["clients"]:
            channels[channel_name]["clients"].append(newclient)

        #RESPUESTA
        tosend = json.dumps({'msg':'connect_channel_ack', 
                                'result':"OK", 
                                "channel_name":channel_name, 
                                "clients_connected":len(channels[channel_name]["clients"])
                                }, encoding='latin1')
        
        #print(tosend)
        #print("Result: " + str(len(result)) )
        
        net.send(addr, tosend)
        
        #avisar a todos los demas clientes que un nuevo integrante se ha sincronizado al channel
        
        #recorrer lista de clientes conectados al canal
        for i in channels[channel_name]["clients"]:

            #dont send notification to the sender
            if i['addr'] != addr:
                
                print("Enviando connection notificacion a: ", i)
                
                tosend = json.dumps({'msg':'new_client_connected', 
                                        'data':data, 
                                        "clients_connected":len(channels[channel_name]["clients"]),
                                        "channel_name":channel_name}, encoding='latin1')
                
                net.send(i['addr'], tosend)
        

    elif data_dict['msg'] == 'connect_channel_ack': 
        
        print('CHANNEL CONNECTION ACK FROM: ', addr, data_dict['result'])
        
        channel_name = data_dict["channel_name"]
        
        #channels[channel_name]["callback_connection"](data_dict['result'], data_dict["clients_connected"])
        Clock.schedule_once(partial(callback_bridge, 
                                    callback = channels[channel_name]["callback_connection"], 
                                    result = data_dict['result'], 
                                    clients_connected = data_dict["clients_connected"]), 0)
                                    
        #start ping interval to the server for this module
        Clock.unschedule(send_ping)
        Clock.schedule_interval(send_ping, 60) #60 seconds, maybe is a good timeout for still active connections
        
        
    elif data_dict['msg'] == 'new_client_connected': 
    
        print('NEW CLIENT CONNECTED FROM: ', addr, data_dict['data'])
        
        channel_name = data_dict["channel_name"]
        
        if channels[channel_name]["callback_new_client_connected"] != None:
            Clock.schedule_once(partial(callback_bridge, 
                                        callback = channels[channel_name]["callback_new_client_connected"],
                                        data = data_dict['data'], 
                                        clients_connected = data_dict["clients_connected"]), 0)
        
    elif data_dict['msg'] == 'write_channel': 
        
        print('CHANNEL WRITE FROM: ', addr)
        
        data = data_dict['data']
        
        channel_name = data_dict["channel_name"]
        
        #if the channel is not in this server, create it
        if channel_name not in channels:
            #create channel without callback
            create_channel(channel_name)
        
        #recorrer lista de clientes conectados al canal
        for i in channels[channel_name]["clients"]:

            #dont send notification to the sender
            if i['addr'] != addr and i['mode'] in ('read', 'readwrite'):
                
                print("Enviando notificacion a: ", i)
                
                tosend = json.dumps({'msg':'inputfrom_channel', 'data':data, "channel_name":channel_name}, encoding='latin1')
                
                net.send(i['addr'], tosend)
                
                
        tosend = json.dumps({'msg':'write_channel_ack', 'request_id':data_dict["request_id"]}, encoding='latin1')
        
        net.send(addr, tosend)
                
    elif data_dict['msg'] == 'write_channel_ack': 
        
        print('CHANNEL WRITE ACK FROM: ', addr)
        
        request_id = data_dict["request_id"]
        
        Clock.schedule_once(partial(callback_bridge, 
                                        callback = write_callbacks[request_id],
                                        request_id = request_id), 0)
            
    elif data_dict['msg'] == 'inputfrom_channel': 
        
        #print('CHANNEL INPUT FROM: ', addr, data_dict['data'])
        
        channel_name = data_dict["channel_name"]
        
        Clock.schedule_once(partial(callback_bridge, 
                                    callback = channels[channel_name]["callback"],
                                    data = data_dict["data"]), 0)
                                    
    elif data_dict['msg'] == 'update_from_client':
        print('Update from client: ', data_dict['data'] )
                
        server_dispatcher_lock.acquire()
        
        server_dispatchers.append( {"function":dispatch_save, "addr":addr, "data_dict":data_dict} )
        
        server_dispatcher_lock.release()
        
        
    elif data_dict['msg'] == 'update_from_client_ack':
        #ejecucion de callback en base al objectId que llamo la actualizacion cloud
        print('Update from client ACK: ', data_dict['objectId'] )
        
        if save_callbacks[data_dict['objectId']] != None:
            Clock.schedule_once(partial(save_callbacks[data_dict['objectId']], data_dict['objectId']), 0)
              
        #sincronizar con los peers que se tiene registro de existencia menor a 3 horas de ultima conexion
        
    elif data_dict['msg'] == 'register_node':
        print("Registrando nodo remoto", addr)
        
        data = data_dict['data']

        node_id = data["node_id"]
        pubkey = data["pubkey"]
        
        if node_id == None:
            node_id = str(uuid.uuid4())
        
        nodes[node_id] = {"addr":addr, "pubkey":pubkey}
        
        #enviar respuesta
        tosend = json.dumps({'msg':'register_node_ack', "data":{'response':"ok_registered", "node_id":node_id} } , encoding='latin1')
        net.send(addr, tosend)
        
    elif data_dict['msg'] == 'register_node_ack':
        
        if callback_registration:
            Clock.schedule_once(partial(callback_registration, data_dict['data']), 0)
        
    
def scanLocalNetwork(callback_found, port=None):
    global net
    global callback_found_device
    global server_port
    
    callback_found_device = callback_found
    
    if port == None:
        port = server_port
    
    net.host_discover(port)

if __name__ == "__main__":
    
    '''
    Cloud basic operations
    '''
    
    init(dbname="testing.db")
    
    myvar = create(className="Post")
    myvar.Title = "Hi this is a post"
    myvar.Message = "Testing the data using our backend"
    myvar.save()
    

