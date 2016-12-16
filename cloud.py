'''
State of the art in "cloud storage" proof of concept technology, pythonic and object oriented

Solucion de almacenamiento de informacion basado en modelo de objetos con 
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


sync_callbacks = {}

def initialized():
    global cnx
    return cnx

def init(dbname='database.db'):
    '''
    Local database initialization, most used on devices
    '''
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
    '''
    Server backend is working under mysql server
    '''
    global cnx
    global tables
    global net
    
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
        net.create_connection(receiver)
            
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
        ngvar = NGVar(className=className, objectId=objectId)
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
    
def sync(className, target_ip=server_ip, **kwargs):
    '''
    Syncs local table and main server table on this device database
    where is the parameters for filter results to minimum values for this user
    latest is the field to test as max latest row
    target_ip is the server for sync
    '''
    print("Sync with: " + target_ip)
    print("Classname: " + className)
    
    sql = kwargs.get('sql')
    params = kwargs.get('params', [])
    
    '''
    where = kwargs.get('where')
    latest = kwargs.get('latest', None)
    
    #get the local top index row for this user-database where condition
    q = Query(className=className)
    q.where( where )    #where can be a dictionary, see the documentation for more details
    
    if latest != None:
        q.orderby( latest , order="DESC")
    
    result = q.find()
    #print(result)
    
    
    if len(result) and latest != None:
        localmaxindex = getattr(result[0], latest)
    else:
        localmaxindex = 0
    
    #print(localmaxindex)
    #raw_input()
    '''
    
    #get the difference between local index and latest index on the remote database
    data = {'className':className, 'sql':sql, 'params':params}
    tosend = json.dumps({'msg':'sync', 'data':data })
    
    if className not in sync_callbacks:
        sync_callbacks[className] = kwargs.get("callback")
    
    net.cb_sync = sync_callback
    
    net.send((target_ip, 31415), tosend)
    
def sync_callback(result, className, dt):
    print("Sync done with: " + className)
    print("Total rows: " + str(len(result)) )
    
    
    for i in result:
        ngvar_item = NGVar()
        ngvar_item.from_values(i)
        ngvar_item.save()
    
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
    cb_func = sync_callbacks[className]
    cb_func(result, dt)
    
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
        self.sql = ""
        self.className = kwargs.get("className")
        self.objectId_key = kwargs.get("objectId_key", None) #this is usefull for compatibility with non standar key fields, is mandatory autoincrement with this mode
        self.objectId = kwargs.get("objectId", "")
                
        self.members_backlist = dir(self)
    
        #if objectId is comming on kwargs, initialize with values from database
        if self.objectId != "":
            #get values from the database
            query = Query(className=self.className)
            query.equalTo("objectId", self.objectId)
            result = query.find()
            if len(result):
                row = result[0]
                
                for i in dir(row):
                    if i not in self.members_backlist:  
                        setattr(self, i, getattr(row, i) )
                        
                    #print(i)
                
    
    def save(self):
        '''
        Insertion and Update in the save function, the object must be valid
        
        Be carefull here, because we are compatible with objectId and non standar, the save
            must avoid problems of fields existence
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
                if i["condition"] in ("=", "<", ">", " LIKE "):
                    self.sql += " AND "  + i["field"] + i["condition"] + "'" + str(i["value"]) + "'" 
                    
                elif i["condition"] in ("ORDER BY", "GROUP BY"):
                    self.sql +=  " " + i["condition"] + " " +  i["field"] + " " + str(i["value"]) + " "
                    
                elif i["condition"] in ("IN"):
                    self.sql +=  " AND "  + i["field"] + " " + i["condition"] + " (" + str(i["value"]) + ") "
                    
                elif i["condition"] in ("LIMIT", "OFFSET"):
                    self.sql +=  " " + i["condition"] + " " + str(i["value"]) + " "
                
        print self.sql
        return self.sql

    def find(self, **kwargs):
        
        raw = kwargs.get("raw", False)
        customsql = kwargs.get("customsql", False)
        
        if customsql == False:
            self.generate_sql()
        
        cursor = cnx.cursor()
        #print( (self.sql) )

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
        
    def like(self, field, value):
        self.conditions.append({"field":field, "condition":" LIKE ", "value":value})
        
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
        
        #print('SIGNUP ACK FROM', addr, data_dict['result'])
        #net.cb_login(data_dict)
        Clock.schedule_once(partial(net.cb_sync, data_dict['result'], data_dict['className']))

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
        q.sql = data['sql'].replace("?", "%s")  #esto es debido a que en app se usa sqlite y en servidor mysql
        q.params = data['params']
        
        #print(q.sql)
        
        '''
        q.where(data['where'])
        if data['latest_field'] != None:
            q.greaterThan(data['latest_field'], data['latest_value'])
        '''
        
        #print("RESULT: ", len(q.find()) )
        
        '''
        #debug
        result = q.find(raw=True)
        for i in result:
            print (i)
            print (json.dumps(i, encoding='latin1'))
            #print (json.dumps(i))
        '''
        
        result = q.find(raw=True, customsql=True)
        
        tosend = json.dumps({'msg':'sync_ack', 'result':result, "className":data['className']}, encoding='latin1')
        
        #print(tosend)
        print("Result: " + str(len(result)) )
        
        net.send(addr, tosend)
        

if __name__ == "__main__":
    
    '''
    Cloud basic operations
    '''
    
    init(dbname="testing.db")
    
    myvar = create(className="Post")
    myvar.Title = "Hi this is a post"
    myvar.Message = "Testing the data using our backend"
    myvar.save()
    

