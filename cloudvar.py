
import json

from network import Network

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


net = Network()
net.create_connection(receiver)

class CloudVar():
	
	def link(self, serverip, data_id):
		pass
		
	def sync(self):
		pass
		
	def unlink(self):
		pass
		
if __name__ == "__main__":
	
	def cb_sync(sync_data):
		print sync_data
	
	print("Probando cloudvars")
	
	net.shutdown_network()
	
	var1 = CloudVar()
	var1.link("127.0.0.1")


	var2 = CloudVar(sync_callback=cb_sync)
	var2.link("127.0.0.1")
	
	var1.colorval = "green"
	var1.sync()
	
	print(var2.colorval)
