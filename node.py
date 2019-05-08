# Azan Bin Zahid - 20100206


# terminal commands
# python node.py new newPort oldPort
# python node.py put newPort oldPort
# python node.py get newPort oldPort


import socket 
import sys 
from _thread import *
import threading 
import pickle
import time
import hashlib 
import binascii
import os
import sys
  


my = {
    "ID" : 0,
    "hash": 0,
    "files": [],
    "path": 0,
    "pred" : 0,
    "succ" : 0,
    "grand_succ": 0,
    "largest" : -1,
    "fingerTable" : [None]*6
}


def hash_func(port):
    m = 6
    size = 2 ** m
    hash_val = binascii.crc32(str(port).encode()) 
    return hash_val % size


# thread fuction 
def incoming_req(c):
    global my

    msg = pickle.loads(c.recv(1024))


    # if alive
    if (msg["req"] == "alive"):
        toSend = {
            "alive": 1,
            "largest": my["largest"],
            "hash": my["hash"]
        }
        c.send (pickle.dumps(toSend))        


    elif (msg["req"] == "get_accepted"):
    
        directory = os.getcwd() + '/' + "get"
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(directory + '/' + msg["filename"], 'wb') as f:
            print ('file opened')
            while True:
                print('receiving data...')
                data = c.recv(1024)
                print('data=%s', (data))
                if not data:
                    break
                # write data to a file
                f.write(data)

        print('Successfully get the file')
        c.close()
        print('connection closed')





    # file transfer
    elif (msg["req"] == "get"):
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        host = '127.0.0.1'

        if(msg["filename"] in my["files"]):

            s.connect((host,msg["port"]))
            toSend = {
                "req": "get_accepted",
                "filename": msg['filename']
            }
            s.send (pickle.dumps(toSend))

            filename=msg["filename"] #In the same folder or path is this file running must the file you want to tranfser to be
            f = open(my["path"] + '/' + filename,'rb')
            l = f.read(1024)
            while (l):
                s.send(l)
                print('Sent ',repr(l))
                l = f.read(1024)
                f.close()

            print('Done sending')
        else:
            s.connect((host, my["succ"]))
            s.send(pickle.dumps(msg))
        
        c.close()
        s.close()

    elif (msg["req"] == "put_accepted"):
    
        directory = os.getcwd() + '/put'
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        # filename = "test.txt" 
        f = open(directory + '/' + msg['filename'],'rb')
        l = f.read(1024)
        while (l):
            c.send(l)
            print('Sent ',repr(l))
            l = f.read(1024)
            f.close()
        c.close()

        print('Done sending')

    elif (msg['req'] == 'finger_succ'):
        if  (msg['key'] <= my["hash"]):
            toSend = {
                "finger_succ": my["ID"]
            }
            c.send (pickle.dumps(toSend))
        else:   
            # s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            # host = '127.0.0.1'
            # s.connect((host, my["succ"]))
            # msg = pickle.loads(s.recv(1024))

            msg = finger_succ(msg['key'])
                     
            # response
            c.send (pickle.dumps(msg))
            
        c.close()
        

    elif (msg["req"] == "put"):
        
        c.close()
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        host = '127.0.0.1'
        filename_hash = hash_func(msg["filename"])

        # more than two nodes
        if (filename_hash <= my["hash"] and filename_hash >= hash_func(my["pred"])) or (my["hash"]==0 and filename_hash > hash_func(my["pred"])):
            
            print(filename_hash, msg)

            s.connect((host,msg["port"]))
            toSend = {
                "req": "put_accepted",
                "filename": msg['filename']
            }
            s.send (pickle.dumps(toSend))

    
            my["files"].append(msg["filename"])

            with open(my["path"] + '/' + msg["filename"], 'wb') as f:
                print ('file opened')
                while True:
                    print('receiving data...')
                    data = s.recv(1024)
                    print('data=%s', (data))
                    if not data:
                        break
                    # write data to a file
                    f.write(data)

            print('Successfully get the file')
            s.close()
            print('connection closed')
        
        else:
            s.connect((host, my["succ"]))
            s.send(pickle.dumps(msg)) 
            s.close()

    
    
    elif (msg["req"] == "update_pred_grand_succ"):
        my["grand_succ"] = grand_succ()
        print (my, "pred grand succ updated")

    # send pred    
    elif (msg["req"] == "get_grand_pred"):
        toSend = my["pred"]
        c.send (pickle.dumps(toSend))
    
    # update grand succ for grand pred
    elif (msg["req"] == "update_grand_pred"):
        my["grand_succ"] = grand_succ()
        print (my, "grand_pred updated")


    # send succ
    elif (msg["req"] == "grand_succ"):
        toSend = my["succ"]
        c.send (pickle.dumps(toSend))

    # inform pred about leaving
    elif (msg["req"] == "inform_pred"):
        if (msg["largest"] != -1):
            my["largest"] = my["hash"]
        my["succ"] = msg ["succ"]
        my["grand_succ"] = grand_succ()
        update_pred_grand_succ()
        print (my, "pred_informed")
    
    # inform succ about leaving
    elif (msg["req"] == "inform_succ"):
        if (msg["hash"]==0):
            my["hash"]=0
        my["pred"] = msg ["pred"]
        print (my, "succ_informed")
    
    
    # accpet as pred
    elif (msg["req"] == "update_pred"):
        my["pred"] = msg ["ID"]
        my["grand_succ"] = grand_succ()
        print (my, "pred_updated")


    # find succ and send
    elif (msg["req"] == "find_succ"):
    
        # if hash collides, discard the connection

        if (msg["hash"] == my["hash"] and msg["ID"] != my["ID"]):
            toSend = {
                "discard": 1
            }
            c.send (pickle.dumps(toSend))
    
        # onyl one node, self connect
        elif (my["ID"] == msg["ID"]):
            toSend = {
                "pred": my["ID"],
                "succ": my["ID"],
                "hash": 0,
                "largest": my["hash"]
            }
            c.send (pickle.dumps(toSend))
        # only two nodes, 
        elif (my["pred"] == my["ID"] and my["succ"] == my["ID"]):
    
            toSend = {
                "pred": my["pred"],
                "succ": my["succ"]
            }

            if (msg["hash"] > my["hash"]):
                # my["largest"] = msg["ID"]
                my["largest"] = -1
                toSend["largest"] = msg["hash"]
            else:
                toSend["largest"] = my["largest"]

            my["pred"] = msg['ID']
            my["succ"] = msg['ID']

            c.send (pickle.dumps(toSend))
            my["grand_succ"] = grand_succ()
            print (my, "two nodes")
        
        # more than two
        else:

    
            if (my["largest"] != -1 and msg["hash"]>my["hash"]):
                toSend = {
                    "pred": my["ID"],
                    "succ": my["succ"],
                    "largest": msg["hash"]
                }
                my["succ"] = msg['ID']
                my["largest"] = -1

                c.send (pickle.dumps(toSend))
                get_grand_pred()
                my["grand_succ"] = grand_succ()
                print (my, "three nodes")
                # update_pred()

            # if between me and succ
            elif(msg['hash']>my["hash"] and msg['hash'] < hash_func(my["succ"] )):
                toSend = {
                    "pred": my["ID"],
                    "succ": my["succ"],
                    # masla
                    "largest": my["largest"]
                }
                my["succ"] = msg['ID']
                c.send (pickle.dumps(toSend))
                get_grand_pred()
                my["grand_succ"] = grand_succ()
                print (my, "three nodes")
                # update_pred()

            else:
                # ask from succ
                c.send (pickle.dumps(ask_succ(msg)))
                # pass
            

    c.close() 
    return


def fail(_):
    global my

    larg = -1
    small = -1

    while(True):
        time.sleep(1)
        try: 
            s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            host = '127.0.0.1'
            s.connect((host,my["succ"]))
            toSend = {
                "req": "alive",
            }
            s.send(pickle.dumps(toSend))
            msg = pickle.loads(s.recv(1024))
            larg = msg["largest"]
            small = msg["hash"]
            s.close()
        
        except:
            if (larg != -1):
                my["largest"] = my["hash"]
            if (small == 0):
                my["hash"] = 0
            # my succ updated
            my["succ"] = my["grand_succ"]
            # tell succ I am pred
            update_pred()
            # my grand succ updated
            my["grand_succ"] = grand_succ()
            
            # tell pred to update grand pred
            update_pred_grand_succ()

            print (my, "fail updated")

    # my["grand_succ"] = grand_succ() 
    # print(my, "pred updated")


def update_pred_grand_succ(node = None):
    global my

    node = node if node!=None else my["pred"]

    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = '127.0.0.1'
    s.connect((host,node))
    
    toSend = {
        "req": "update_pred_grand_succ",
    }
    s.send(pickle.dumps(toSend))
    s.close()
    # my["grand_succ"] = grand_succ() 
    # print(my, "pred updated")


def update_pred(node = None):
    global my

    node = node if node!=None else my["succ"]
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = '127.0.0.1'
    s.connect((host,node))
    
    toSend = {
        "req": "update_pred",
        "ID": my["ID"],
    }
    s.send(pickle.dumps(toSend))
    s.close()
    # my["grand_succ"] = grand_succ() 
    # print(my, "pred updated")

def get_grand_pred():
    global my
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = '127.0.0.1'
    s.connect((host,my["pred"]))

    # req succ from exisitng node
    toSend = {
        "req": "get_grand_pred",
    }
    s.send(pickle.dumps(toSend))
    msg = pickle.loads(s.recv(1024))
    # new added
    s.close()

    # ask grand pred to update your grand succ

    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = '127.0.0.1'
    s.connect((host,msg))

    # req succ from exisitng node
    toSend = {
        "req": "update_grand_pred",
    }
    s.send(pickle.dumps(toSend))
    # new added
    s.close()


# grand succ
def grand_succ():
    global my
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = '127.0.0.1'
    s.connect((host,my["succ"]))

    # req succ from exisitng node
    toSend = {
        "req": "grand_succ",
    }
    s.send(pickle.dumps(toSend))
    msg = pickle.loads(s.recv(1024))
    # new added
    s.close()
    return msg

# ask succ
def ask_succ(msg):
    global my
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = '127.0.0.1'

    # iterate in finger tabel 
    nxt = my["succ"]
    for e in my["fingerTable"][::-1]:
        if (msg["hash"]<e[0]):
            nxt = e[1]


    s.connect((host,nxt))

    # req succ from exisitng node
    toSend = msg
    s.send(pickle.dumps(toSend))
    msg = pickle.loads(s.recv(1024))
    # new added
    s.close()
    return msg

def inform_pred():
    global my

    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = '127.0.0.1'
    s.connect((host,my["pred"]))
    
    toSend = {
        "req": "inform_pred",
        "ID": my["ID"],
        "succ": my["succ"],
        "largest": my["largest"],
        "hash": my["hash"]
    }
    s.send(pickle.dumps(toSend))
    s.close()

def inform_succ():
    global my

    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = '127.0.0.1'
    s.connect((host,my["succ"]))
    
    toSend = {
        "req": "inform_succ",
        "ID": my["ID"],
        "pred": my["pred"],
        "hash": my["hash"],

    }
    s.send(pickle.dumps(toSend))
    s.close()

def fingerTable(_):
    print ("finger_call")
    global my
    while(True):
        time.sleep(5)
        # my["fingerTable"][0] = my
        for i in range(6):
            key = (my["hash"] + 2 ** i) % 2**6
            my["fingerTable"][i] = [ key, finger_succ(key)['finger_succ']]
        print (my, "finger tabel update")

def finger_succ(key):
    global my

    if  ((key <= my["hash"] and  key >= hash_func(my["pred"]) ) or (my['hash']==0 and my['largest']!=-1) or (my["hash"]==0 and key > hash_func(my["pred"])) ):
        # (my["hash"]==0 and filename_hash > hash_func(my["pred"])
    # if ((key <= my["hash"] and (my["hash"]==0 and key >= hash_func(my["pred"]) )) or (my['hash']==0 and my['largest']!=-1)):
        msg = {
            "finger_succ": my["ID"]
        }
        return msg

    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = '127.0.0.1'
    try:
        s.connect((host,my["succ"]))
    except:
        msg = {
            "finger_succ": my["ID"]
        }
        return msg

    # req pred and succ from exisitng node
    toSend = {
        "req": "finger_succ",
        "key": key,
    }
    s.send(pickle.dumps(toSend))
    msg = pickle.loads(s.recv(1024))
    s.close()

    return msg


def leave(_):
    if (input("enter 'leave' if you want to leave \n") == 'leave'):
        inform_pred()
        inform_succ()
        for f in my["files"]:
            put_file(my['succ'], f)
        os._exit(1)

# thread fuction 
def connect_to_DHT(existing_port):
    global my

    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = '127.0.0.1'
    s.connect((host,existing_port))

    # req pred and succ from exisitng node
    toSend = {
        "req": "find_succ",
        "ID": my["ID"],
        "hash": my["hash"]
    }
    s.send(pickle.dumps(toSend))

    # recv pred and succ
    msg = pickle.loads(s.recv(1024))

    if 'discard' in msg:
        print ("connection refused, change port")
        s.close()
        return

    my["pred"] = msg["pred"]
    my["succ"] = msg["succ"]
    my["largest"] = msg["largest"] if msg["largest"] == my["hash"] else -1
    if 'hash' in msg:
        my["hash"] = msg['hash']
        my["largest"] = msg['hash']

    # create directory 
    # os.chdir(os.path.dirname(__file__))
    # print(os.getcwd())

    directory = os.getcwd() + '/' + str(my["hash"])
    if not os.path.exists(directory):
        os.makedirs(directory)
    my["path"] = directory

    update_pred()
    get_grand_pred()
    my["grand_succ"] = grand_succ()
    print(my, "rcvd")
    start_new_thread(fingerTable, (0,)) 
    start_new_thread(leave, (0,)) 
    start_new_thread(fail, (0,)) 

    # sai nh hai abi
    s.close()

def put_file(existing_port, filename = None):
    global my

    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = '127.0.0.1'

    # name = input("Enter Filename: ")

    name = filename if filename!=None else input('filename please? ')
    # name = "test - Copy.txt"
    s.connect((host,existing_port))

    # req pred and succ from exisitng node
    toSend = {
        "req": "put",
        "filename": name,
        "port": my["ID"],
    }
    s.send(pickle.dumps(toSend))
    s.close()


def get_file(existing_port, filename = None):
    global my

    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    host = '127.0.0.1'

    # name = input("Enter Filename: ")

    name = filename if filename!=None else input('filename please? ')
    # name = "test - Copy.txt"
    s.connect((host,existing_port))

    # req pred and succ from exisitng node
    toSend = {
        "req": "get",
        "filename": name,
        "port": my["ID"],
    }
    s.send(pickle.dumps(toSend))
    s.close()
  
def Main(): 
    global my

    # 1st arg type of node
    # 2nd arg port number
    # 3rd arg existing port number

    # print "This is the name of the script: ", sys.argv[0]
    # print "Number of arguments: ", len(sys.argv)
    # print "The arguments are: " , str(sys.argv)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    host = ''
    port = int(sys.argv[2])
    my["ID"] = port
    my["hash"] = hash_func(port)

    s.bind((host, port)) 
    # print("socket binded to post", port) 

    s.listen(100) 
    print("socket is listening") 

    if (sys.argv[1]=="new"):
        start_new_thread(connect_to_DHT, (int(sys.argv[3]),)) 
    
    # put file in node
    elif (sys.argv[1]=="put"):
        # my["join_only"]=1
        start_new_thread(put_file, (int(sys.argv[3]),)) 

    # put file in node
    elif (sys.argv[1]=="get"):
        # my["join_only"]=1
        start_new_thread(get_file, (int(sys.argv[3]),)) 

    while True: 
        # establish connection with client 

        c, addr = s.accept()   
        # print('Node','connected to :', addr[0], ':', addr[1]) 
        start_new_thread(incoming_req, (c,)) 

    s.close() 
  
  
if __name__ == '__main__': 
    Main() 