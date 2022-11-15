import sys
import random
import time
import grpc
import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2
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
def request_vote(node_id):
    if not asleep and state == State.candidate:
            global term_number, id
            addr = f'{server_address[node_id][0]}:{server_address[node_id][1]}'
            ch = grpc.insecure_channel()
            stub = pb2_grpc.RaftServiceStub(ch)

            resp = stub.AskVote(pb2.NodeInfo(term= term_number, id = id))
            voted = resp.result
            if voted:
                global n_votes
                n_votes += 1
            if resp.term > term_number:
                state = State.follower
                term_number = resp.term
                state_out()


def heartbeat(node_id):
    if not asleep and state == State.leader:
            global term_number, id
            addr = f'{server_address[node_id][0]}:{server_address[node_id][1]}'
            ch = grpc.insecure_channel(addr)
            stub = pb2_grpc.RaftServiceStub(ch)


            resp = stub.AppendEntries(pb2.NodeInfo(term=term_number, id=id))
            success= resp.result
            if not success:
                global state, term_number
                term_number= resp.term
                state = State.follower
                state_out()



def leader_job():
    while True:
        if not asleep and state == State.leader:
            send_to_all(heartbeat)
            time.sleep(0.05)

# Help func to multithread sending msg


def send_to_all(func):
    servers = []
    for n in server_address:
        servers.append(Thread(target=func, args=(n,)))
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


def get_leader():
    if not asleep:
        result = f'{leader_info[0]} {leader_info[1]}:{leader_info[2]}'
        print(result)
        return leader_info[0], f"{leader_info[1]}:{leader_info[2]}"

timer_t = Thread(target=timer_thr)
lead_t = Thread(target=leader_job)


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

class Handler(pb2_grpc.RaftServiceServicer):

    def AskVote(self, request, context):
        res= vote_check(request.term, request.id)
        return pb2.VoteResult(term=res[0],result= res[1])

    def AppendEntries(self, request, context):
       res= AppendEntries(request.term, request.id)
       return pb2.VoteResult(term=res[0],result= res[1])

    def Suspend(self, request, context):
        suspend(request.period)
    
    def GetLeader(self, request, context):
        id, ip= get_leader()
        return pb2.LeaderInfo(id=id,address=ip)