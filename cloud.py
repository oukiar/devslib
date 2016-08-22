'''
State of the art in "cloud storage" proof of concept technology, pythonic and object oriented

Primer solucion de almacenamiento de informacion basado en modelo de objetos con 
cloud on/offline automatico y sync basado en versiones con solucion automatica de colisiones (duplicacion)

Se cuenta con un esquema distribuido de la base de datos en relacion a los users y devices
'''

import json
import uuid

try:
    from kivy.clock import Clock
    from functools import partial

except:
    Clock = None

#disabled, schema must be created in runtime with creation of vars
#from tables import schema

        

#session token
session_token = None
#server_ip = "104.236.181.245"
server_ip = "162.243.152.20"
user = None
net = None

class NGVar:
    '''
    Another try for netget variable
    '''
    def __init__(self, **kwargs):
        self.sql = ""
        self.className = kwargs.get("className")
        self.objectId = kwargs.get("objectId", "")
                
        self.members_backlist = dir(self)
    
        #if objectId is comming on kwargs, initialize with values from database
        if self.objectId != "":
            #get values from the database
            query = cloud.Query(className=self.className)
            query.equalTo("objectId", self.objectId)
            result = query.find()
            if len(result):
                row = result[0]
                '''
                for i in dir(row):
                    print(i)
                '''
    
    def save(self):
        '''
        Insertion and Update in the save function, the object must be valid
        '''
        if self.objectId != "":
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
                    else:
                        values += ", " + i + "=?" #+ val
            
            sql += values + " where objectId='"+ self.objectId + "'"  
            
            
            try:
                cursor = cnx.cursor()
                if cursor.execute(sql, lst_values):
                    #print("SQL: " + sql)
                    
                    if autocommit:
                        cnx.commit()
                        
                    return True
                                      
            except sqlite3.Error as e:
            
                if 'no such table' in e.args[0]:
                    print('sqlite3 Error: No such table')
                    print (e.args[0])
                else:
                    print('sqlite3 Error: Unknown error')
                    print (e.args[0])
                                
                print("Error updating-saving: " + sql)
                print("Values: " + json.dumps(lst_values) )
            
            return False
        else:
            #create object unique ID
            self.objectId = str(uuid.uuid4())
            
            
            if self.className not in tables:
                
                tables.append(self.className)
                
                #create table
                sql = "CREATE TABLE %s (objectId TEXT PRIMARY KEY NOT NULL" % (self.className)
                
                print("Recorriendo elementos de self")
                
                #recorrer con dir los elementos de nuestro objeto, para saber que campos llevara
                for i in dir(self):
                    if i not in self.members_backlist and i != "members_backlist":
                        
                        #print(i, type(getattr(self, i) ) )
                        
                        if str(type(getattr(self, i) )) in ["<type 'str'>", "<type 'unicode'>", "<class 'str'>"]:
                            tp = "TEXT"
                        elif str(type(getattr(self, i) )) in ["<type 'int'>", "<class 'int'>"]:
                            tp = "INT"
                        else:
                            tp = "" #esto quizas provoca error al no definir tipo del campo
                        
                        #
                        sql += ", " + i + " " + tp
                
                sql += ");"
                
                #crear tabla
                cursor = cnx.cursor()
                
                if cursor.execute(sql):
                    print("Tabla creada")
                else:
                    print("Error creando tabla SQL: " + sql)
            else:
                pass
                #print("Table already exists")
                
            
            '''
            #check if table already exists
            sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='%s'" % self.className
            #print("Checking: " + sql)
            
            cursor = cnx.cursor()
            if cursor.execute(sql):
                
                #if table does not exists
                if len(cursor.fetchall() ) == 0:
                    
                    #create table
                    sql = "CREATE TABLE %s (objectId TEXT PRIMARY KEY NOT NULL" % (self.className)
                    
                    print("Recorriendo elementos de self")
                    
                    #recorrer con dir los elementos de nuestro objeto, para saber que campos llevara
                    for i in dir(self):
                        if i not in self.members_backlist and i != "members_backlist":
                            
                            #print(i, type(getattr(self, i) ) )
                            
                            if str(type(getattr(self, i) )) in ["<type 'str'>", "<type 'unicode'>", "<class 'str'>"]:
                                tp = "TEXT"
                            elif str(type(getattr(self, i) )) in ["<type 'int'>", "<class 'int'>"]:
                                tp = "INT"
                            else:
                                tp = "" #esto quizas provoca error al no definir tipo del campo
                            
                            #
                            sql += ", " + i + " " + tp
                    
                    sql += ");"
                    
                    #crear tabla
                    cursor = cnx.cursor()
                    
                    if cursor.execute(sql):
                        print("Tabla creada")
                    else:
                        print("Error creando tabla SQL: " + sql)
                else:
                    print("Table already exists")
            '''
            
            
            #insert
            sql = "insert into " + self.className + "(objectId"
            
            values = ""
            lst_values = []
            
            for i in dir(self):
                if i not in self.members_backlist and i != "members_backlist":
                    sql += ", " + i #HERE: avoid the sqlinjection
                    
                    #print (str(type(getattr(self, i) )))
                    values += ", ?" 
                    lst_values.append(getattr(self, i))
                    
                    '''
                    if str(type(getattr(self, i) )) in ["<type 'int'>", "<class 'int'>"]:
                        values += ", " + str(getattr(self, i))
                    else:
                        try:
                            values += ", '" + str(getattr(self, i) ) + "'"
                        except:
                            values += ", '" + str(getattr(self, i).encode('utf8') ) + "'"
                    '''
                    
            sql += ") values('"+ self.objectId + "'" + values + ")" 
            
            
            try:
                cursor = cnx.cursor()
                
                #print("SQL: " + sql)
                
                if cursor.execute(sql, lst_values):
                    if autocommit:
                        cnx.commit()
                    return True
                    
            except sqlite3.Error as e:
            
                if 'no such table' in e.args[0]:
                    print('sqlite3 Error: No such table')
                    print (e.args[0])
                else:
                    print('sqlite3 Error: Unknown error')
                    print (e.args[0])
                
                #print("Error saving: " + sql)
                #input()
            
            return False

    def delete(self, destroy=True):
        '''
        By default delete the row ... 
        '''
        sql = "delete from " + self.className + " where objectId='" + self.objectId + "'"
        print("Deleting: " + sql)
        
        cursor = cnx.cursor()
        if cursor.execute(sql):
            #print("SQL: " + sql)
            if autocommit:
                cnx.commit()
            return True
        
        return False

    def destroy(self):
    	'''
    	This will really delete the register
    	'''
    	pass
        
        
    def create_ss(self, arr):
        ss = ""
        for i in range(0, len(arr)):
            if ss != "":
                ss += ","
                
            if pymysql != None:
                ss += "%s"
            else:
                ss += "?"
            
            
        return ss
        
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
            print(i)
            setattr(self, i, values[i])
        

#-----------------
#MODULE FUNCTIONS

#use sqlite for local storage
import sqlite3

try:
    import pymysql
except:
    pymysql = None

cnx = None
tables = []
autocommit = True

def init(dbname='database.db'):
    global cnx
    global tables
    global net
    
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

    #network
    net = Network()  
    net.create_connection(receiver)
    
def init_server():
    global cnx
    global tables
    
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
            
        print("Orgboat server init success")
            
    else:
        print("Error initializing server")

def create(className, objectId=None):
    '''
    Create using their objectId

    If classname does not exist, it is created as a table on the database
    '''
    if objectId != None:
        '''
        Create for and existing register on the database
        '''
        pass
    else:
        ngvar = NGVar()
        ngvar.className = className
        
        
    return ngvar

def login(username, password, **kwargs):
    '''
    Request login always on the main server
    '''
    data = {'username':username, 'password':password}

    tosend = json.dumps({'msg':'login', 'data':data })

    net.cb_login = kwargs.get("callback")
    net.send((server_ip, 31415), tosend)
    
def signup(username, password, email, **kwargs):
    '''
    Request signup always on the main server
    '''
    data = {'username':username, 'password':password, 'email':email}

    tosend = json.dumps({'msg':'signup', 'data':data })

    net.cb_signup = kwargs.get("callback")
    net.send((server_ip, 31415), tosend)

def quit():
    net.shutdown_network()
    
def sync(className, target_ip=server_ip, **kwargs):
    '''
    Syncs local table and main server table on this device database
    where is the parameters for filter results to minimum values for this user
    latest is the field to test as max latest row
    target_ip is the server for sync
    '''
    print("Sync with: " + target_ip)
    print("Classname: " + className)
    
    where = kwargs.get('where')
    latest = kwargs.get('latest')
    
    #get the local top index row for this user-database where condition
    q = Query(className=className)
    q.where( where )    #where can be a dictionary, see the documentation for more details
    q.orderby( latest )
    result = q.find()
    print(result)
    
    if len(result):
        localmaxindex = result[0]
    else:
        localmaxindex = 0
    
    #get the difference between local index and latest index on the remote database
    data = {'className':className, 'where':where, 'latest_field':latest, 'latest_value':localmaxindex }
    tosend = json.dumps({'msg':'sync', 'data':data })
    net.cb_sync = kwargs.get("callback")
    net.send((target_ip, 31415), tosend)
    
def get_max(className, field):
    '''
    Get the max value of all rows on field
    '''
    query = Query(className=className)
    query.orderby(field, order="DESC")
    res = query.find()
    
    if len(res):
        return getattr(res[0], field)
    
    return 0
    

class Query:
    '''
    This is the class that will let you get data from the cloud
    '''
    def __init__(self, **kwargs):
        self.className = kwargs.get('className', False)
        if self.className:
            self.sql = "select * from " + self.className
            self.params = []
            self.conditions = ""
            self.order = ""
            self.group = ""

    def equalTo(self, field, value):
        if self.conditions != "":
            self.conditions += " AND "
        
        if pymysql != None:
            self.conditions += "`" + field + "`=%s"
        else:
            self.conditions += field + "=?"
        
        
        #self.conditions += field + "=?"
            
        #self.params.append(field)
        self.params.append(value)

    def find(self, **kwargs):
        
        raw = kwargs.get("raw", False)
        
        if self.conditions != "":
            self.sql += " where " + self.conditions 
            self.conditions = ""
        
        if self.order != "":
            self.sql += " " + self.order
            self.order = ""
            
        if self.group != "":
            self.sql += " " + self.group
            self.group = ""
        
        cursor = cnx.cursor()
        print( (self.sql, self.params) )

        try:
            cursor.execute(self.sql, self.params)
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
        
    def orderby(self, field, order='ASC'):
        if self.order == "":
            self.order = " ORDER BY " + field + " " + order + " "
        else:
            self.order += ", " + field + " " + order + " "
            
    def groupby(self, field):
        if self.group == "":
            self.group = " GROUP BY " + field + " "
        else:
            self.group += ", " + field + " "
        
    def where(self, where_dict):
        #print('WHERE')
        for i in where_dict:
            #print(i)
            self.equalTo(i, where_dict[i])
            
    def greaterThan(self, field, value):
        #if conditions are yet initialized
        if self.conditions != "":
            self.conditions += " AND "
            
        
        if pymysql != None:
            self.conditions += " `" + field + "`>%s"
        else:
            self.conditions += ' ' + field + '>?'
        
        
        #self.conditions += ' ' + field + '>?'
        
        self.params.append(value)        



#--------------------------------------
#NETWORKING STUFF
#all this work is only for low level communications of cloud synchronizations
#

from devslib.network import Network
import socket


try:
    import bcrypt
except:
    bcrypt = None

def receiver(data, addr):
    data_dict = json.loads(data)

    if data_dict['msg'] == 'ping_ack':        
        #print('PING ACK RECEIVED FROM ', addr, data_dict['data'])
        
        if addr[0] != net.ngsock.addr[0]:
        	pass
            #Clock.schedule_once(partial(self.add_devicehost, addr, data_dict['data']), 0)
        

    elif data_dict['msg'] == 'ping':
        
        print('PING RECEIVED FROM ', addr, data_dict['data'])
        
        tosend = json.dumps({'msg':'ping_ack', 'data':socket.gethostname()})
        net.send(addr, tosend)
        
    elif data_dict['msg'] == 'signup_ack':
        
        #print('SIGNUP ACK FROM', addr, data_dict['data'])
        #net.cb_signup(data_dict)
        Clock.schedule_once(partial(net.cb_signup, data_dict))
        
    elif data_dict['msg'] == 'login_ack':
        
        #print('SIGNUP ACK FROM', addr, data_dict['data'])
        #net.cb_login(data_dict)
        Clock.schedule_once(partial(net.cb_login, data_dict))

    elif data_dict['msg'] == 'sync_ack':
        
        #print('SIGNUP ACK FROM', addr, data_dict['data'])
        #net.cb_login(data_dict)
        Clock.schedule_once(partial(net.cb_sync, data_dict))

    #------------------
    #ONLY SERVERS ACTIONS
    elif data_dict['msg'] == 'signup':
        
        print('SIGNUP FROM ', addr, data_dict['data'])
        
        #try to sign up this new user
        print(data_dict['data']['username'])

        q = Query(className="users")
        q.equalTo('username', data_dict['data']['username'])
        result = q.find()

        #check that username does not exist
        if len( result ):
            #user already exists

            tosend = json.dumps({'msg':'signup_ack', 'data':"already_used"})
            net.send(addr, tosend)
            return
            
        sessiontoken = os.urandom(32)

        #new user initialization
        newuser = create("users")
        newuser.setval('username', data_dict['data']['username'])
        newuser.setval('password', data_dict['data']['password'])
        newuser.setval('email', data_dict['data']['email'])
        newuser.setval('sessiontoken', sessiontoken)

        #it is done in the server, can be syncronously
        if(newuser.save() ):
            tosend = json.dumps({'msg':'signup_ack', 'result':"welcome", "newuser":json.dumps(newuser.values)})
        else:
            tosend = json.dumps({'msg':'signup_ack', 'result':"error", "errormsg":newuser.error})

        net.send(addr, tosend)
        
    elif data_dict['msg'] == 'login':
        
        print('LOGIN FROM ', addr, data_dict['data'])
        
        #try to sign up this new user
        print(data_dict['data']['username'])

        q = Query(className="users")
        q.equalTo('username', data_dict['data']['username'])
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
            #if data_dict['data']['password'] == user.getval("password"):            
            if bcrypt.hashpw( data_dict['data']['password'].encode("latin1"), hashed ) == hashed:
                
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


    elif data_dict['msg'] == 'sync':
        
        print('SYNC FROM ', addr, data_dict['data'])

        data = data_dict['data']
        
        #obtener los registros mayores al ultimo existente en el cliente remoto
        q = Query(className=data['className'])
        q.where(data['where'])
        q.greaterThan(data['latest_field'], data['latest_value'])
        #print("RESULT: ", len(q.find()) )
        
        '''
        #debug
        result = q.find(raw=True)
        for i in result:
            print (i)
            print (json.dumps(i, encoding='latin1'))
            #print (json.dumps(i))
        '''
        
        result = q.find(raw=True)
        
        tosend = json.dumps({'msg':'sync_ack', 'result':result, "className":data['className']}, encoding='latin1')
        
        #print(tosend)
        print("Result: " + str(len(result)) )
        
        net.send(addr, tosend)
        
    

