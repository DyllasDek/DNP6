import sys
import random
import time
import grpc
#import chord_pb2_grpc as pb2_grpc
#import chord_pb2 as pb2
from enum import Enum
from datetime import datetime
from threading import Thread

# Enumerator for better state change


State = Enum('State', 'follower candidate leader', start=1)

# Initial values of server
id = int(sys.argv[1])
timer = random.randrange(150, 301) * 0.001
term_number = 0
voted = False
asleep = False
state = State.follower
n_votes = 0
n_nodes = 0
timer_restart = False
ip = None
port = None
leader_info = (-1, -1, -1)

# Server adress pool
server_address = {}

# Parse config file
for line in open('config.conf', 'r').readlines():
    id_, ip_, port_ = line.split(" ")
    server_address[int(id_)] = (ip_, int(port_))

# Check if given id exists in congin.conf
if id not in server_address:
    print(f'Wrong ID')
    sys.exit()
else:
    # Save number of all servers and save server's ip and port
    n_nodes = len(server_address.keys())
    ip, port = server_address[id]
    del server_address[id]



# Prints state of server
def state_out():
    print(f'I am a {state.name}. Term: {term_number}\n')


# Thread timer function. Changes states of server if needed
def timer_thr():
    start = datetime.now()
    while True:
        # If time to restart timer - restart it by changing start point
        if (timer_restart):
            start, timer_restart = datetime.now(), False

        # If we get end of time, review server state
        elif ((datetime.now() - start).total_seconds() >= timer):
            # If leader, just restart timer
            if state == State.leader:
                timer_restart = True
            # If candidate - check votes and change state
            elif state == State.candidate:
                cond = n_votes >= n_nodes//2 + 1
                state = State.leader if cond else State.follower
                timer_restart = True
                state_out()
            # If follower and no hearth beat - start elections
            elif state == State.follower:
                start_elections()



# Check vote for specific id
# If itself - vote itself
def vote_check(term, id):
    # Assign global variables
    global term_number,leader_info,state,timer_restart

    if not asleep:
        reply = None
        timer_restart = True
        if term > term_number:
            term_number = term
            voted = False
            reply = False
        if term_number == term and not voted:
            voted = True
            state = State.follower
            leader_info = (id, server_address[id][0], server_address[id][1])
            print(f'Voted for node {id}')
            reply = True
        return term_number, reply


# Func for threads requests. Get votes and calculate states
def request_vote(id):
    if not asleep and state == State.candidate:
            global term_number
            addr = f'{server_address[id][0]}:{server_address[id][1]}'
            ch = grpc.insecure_channel()
            stub = pb2_grpc.SimpleServiceStub(ch)

            # stub, Аня, вверяю тебе

            # resp = stub.
            # if resp.voted:
            #   global n_votes
            #   n_votes += 1
            # if resp.term > term_number:
            #   global term_number
            #   state = States.follower
            #   term_number = term
            #   state_out()


def heartbeat():
    if not asleep and state == State.leader:
            global term_number
            addr = f'{server_address[id][0]}:{server_address[id][1]}'
            ch = grpc.insecure_channel()
            stub = pb2_grpc.SimpleServiceStub(ch)

            # stub, Аня, вверяю тебе

            #resp = stub.AppendEntries(...)
            # if not resp.success:
            #   global state
            #   state = States.follower
            #   state_out()


def leader_job():
    while True:
        if not asleep and state == State.leader:
            send_to_all(heartbeat)
            time.sleep(0.05)

# Help func to multithread sending msg


def send_to_all(func):
    servers = []
    for n in server_address:
        ip, port = server_address[n]
        servers.append(Thread(target=func, args=(f'{ip}:{port}',)))
    [t.start() for t in servers]
    [t.join() for t in servers]


# Start election process, increase term, ask for votes
def start_elections():
    if not asleep:
        global state
        global term_number
        print("The leader is dead")
        # Change curr state
        state = State.candidate
        term_number += 1
        state_out()
        # Check votes, if not voted - give myself vote
        vote_check(term_number, id)
        # Send to all servers request for vote
        send_to_all(request_vote)
        print("Votes received")
        # If state not changed and get enough votes - leader
        # If not, timer will give us follower state
        if state == State.Candidate and n_votes >= n_nodes//2 + 1:
            global timer_restart
            timer_restart = True
            state = State.Leader
            state_out()


def suspend(period):
    if not asleep:
        print(f"Sleeping for {period} seconds")
        asleep = True
        time.sleep(period)
        asleep = False


def get_leader(self):
    if not asleep:
        result = f'{leader_info[0]} {leader_info[1]}:{leader_info[2]}'
        print(result)
        return result

timer_t = Thread(target=timer_thr)
lead_t = Thread(target=leader_job)

""" 
Если вкратце, append_entries в хенделере должен 
менять состояние сервера, примерный пример кода:

AppendEntries(term, leaderId)
if not asleep:
    global timer_restart
    timer_restart = True
    if term >= term_number:
        updated_state = True if term > term_number else False
        if leaderId is not leader_info[0]:
            global leader_info
            ip,port = server_address[leaderId]
            leader_info = (leaderId, ip, port)
        global term_number
        term_number = term
        state = State.Follower
        if updated_state:
            state_out()
        return (term_number, True)
    return (term_number, False)
"""

def AppendEntries(term, leader_id):
    if asleep:
        return
      
    global timer_restart, leader_info, term_number, state, need_state_update
    timer_restart = True
    if term >= term_number:
        need_state_update= term > term_number
        if leader_id is not leader_info[0]:
            ip,port = server_address[leader_id]
            leader_info = (leader_id, ip, port)
        term_number = term
        state = State.Follower
        if need_state_update:
            state_out()
        return (term_number, True)
    return (term_number, False)
# Ну а остальное клиентское, там просто запросы по функциям 

"""
class Handler(pb2_grpc.SimpleServiceServicer):

    def ReloadTable(self, request, context):
        Update()
        return pb2.GetInfo()

    def GetType(self, request, context):
        return pb2.TypeReply(type="Node")

    def Find(self, request, context):
        flag, reply = find(request.key)
        msg = ""
        if flag:
            msg = f'{reply[0]}||{reply[1]}'
        else:
            msg = reply
        return pb2.SRFReply(reply=msg, success=flag)

    def FindKey(self, request, context):
        flag, reply = find(request.key)
        if reply[0] == nodeid:
            if request.key in data:
                return pb2.SRFReply(reply=f"{request.key} is saved in {nodeid}", success=True)
        else:
            ch = grpc.insecure_channel(reply[1])
            f_stub = pb2_grpc.SimpleServiceStub(ch)
            repl = f_stub.GetKeysText(pb2.GetInfo())
            if request.key in repl.keys:
                return pb2.SRFReply(reply=f"{request.key} is saved in {reply[0]}", success=True)
        return pb2.SRFReply(reply=f'{request.key} does not exist in node {reply[0]}', success=False)

    def SaveFromClient(self, request, context):
        tup = save(request.key, request.text)
        return pb2.SRFReply(reply=tup[1], success=tup[0])

    def RemoveFromClient(self, request, context):
        tup = remove(request.key)
        return pb2.SRFReply(reply=tup[1], success=tup[0])

    def Save(self, request, context):
        if (request.key in data):
            return pb2.SRFReply(reply=f"{request.key} already exists in node {nodeid}", success=False)
        data[request.key] = request.text
        return pb2.SRFReply(reply=f"{request.key} is saved in node {nodeid}", success=True)

    def Remove(self, request, context):
        if (request.key not in data):
            return pb2.SRFReply(reply=f"{request.key} does not exist in node {nodeid}", success=False)
        del data[request.key]
        return pb2.SRFReply(reply=f"{request.key} is removed from {nodeid}", success=True)

    def GetNode(self, request, context):
        msg = []
        for key in finger_table:
            msg.append(f'{key}:   {finger_table[key]}')
        return pb2.GetNodeChordReply(id=nodeid, table=msg)

    def GetKeysText(self, request, context):
        msg = []
        k = GetKeys()
        for key in k:
            msg.append(key)
        return pb2.KeysTextReply(keys=msg)
"""
