import sys
import random
import time
import grpc
import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2
from enum import Enum
from datetime import datetime
from threading import Thread
from concurrent import futures

# Enumerator for better state change


State = Enum('State', ['follower', 'candidate', 'leader'], start=1)

# Initial values of server
id = int(sys.argv[1])
timer = random.randrange(150, 300) * 0.001
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
dead = False

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
    print(f'The server starts at {ip}:{port}')


# Prints state of server
def state_out():
    print(f'I am a {state.name}. Term: {term_number}\n')


# Thread timer function. Changes states of server if needed
def timer_thr():
    start = datetime.now()
    global n_votes, timer_restart, state, dead
    while True:
        # If time to restart timer - restart it by changing start point
        if dead:
            return
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
                n_votes = 0
                state_out()
            # If follower and no hearth beat - start elections
            elif state == State.follower:
                n_votes = 0
                start_elections()


# Check vote for specific id
# If itself - vote itself
def vote_check(term, id):
    # Assign global variables
    global term_number, leader_info, state, timer_restart, voted, server_address
    try:
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
                leader_info = (
                    id, server_address[id][0], server_address[id][1])
                print(f'Voted for node {id}')
                reply = True
            return term_number, reply
    except:
        return (-1, False)


# Func for threads requests. Get votes and calculate states
def request_vote(node_id):
    global term_number, id, n_votes, state
    try:
        if not asleep and state == State.candidate:
            addr = f'{server_address[node_id][0]}:{server_address[node_id][1]}'
            ch = grpc.insecure_channel(addr)
            stub = pb2_grpc.RaftServiceStub(ch)

            resp = stub.AskVote(pb2.NodeInfo(term=term_number, id=id))
            voted = resp.result
            if voted:
                n_votes += 1
            if resp.term > term_number:
                state = State.follower
                term_number = resp.term
                state_out()

    except:
        return


def heartbeat(node_id):
    try:
        global state, term_number, id
        if not asleep and state == State.leader:
            addr = f'{server_address[node_id][0]}:{server_address[node_id][1]}'
            ch = grpc.insecure_channel(addr)
            stub = pb2_grpc.RaftServiceStub(ch)
            resp = stub.AppendEntries(pb2.NodeInfo(term=term_number, id=id))
            success = resp.result
            if not success:
                term_number = resp.term
                state = State.follower
                state_out()
    except:
        return


def leader_job():
    while True:
        if dead:
            return
        try:
            if not asleep and state == State.leader:
                send_to_all(heartbeat)
                time.sleep(0.05)
        except:
            continue


# Help func to multithread sending msg
def send_to_all(func):
    if dead:
        return
    servers = []
    for n in server_address:
        servers.append(Thread(target=func, args=(n,)))
    [t.start() for t in servers]
    [t.join() for t in servers]


# Start election process, increase term, ask for votes
def start_elections():
    if not asleep:
        global state, term_number, timer_restart, n_votes
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
        if state == State.candidate and n_votes >= n_nodes//2 + 1:
            timer_restart = True
            state = State.leader
            state_out()


def suspend(period):
    global asleep
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


def client_commands(msg):
    print(f'Command from client: {msg}')


def AppendEntries(term, leader_id):
    if asleep:
        return

    global timer_restart, leader_info, term_number, state, need_state_update
    timer_restart = True
    if term >= term_number:
        need_state_update = term > term_number
        if leader_id is not leader_info[0]:
            ip, port = server_address[leader_id]
            leader_info = (leader_id, ip, port)
        term_number = term
        state = State.Follower
        if need_state_update:
            state_out()
        return (term_number, True)
    return (term_number, False)


class Handler(pb2_grpc.RaftServiceServicer):

    def CheckAlive(self, request, context):
        return pb2.EmptyMessage()

    def AskVote(self, request, context):
        res = vote_check(request.term, request.id)
        return pb2.VoteResult(term=res[0], result=res[1])

    def AppendEntries(self, request, context):
        res = AppendEntries(request.term, request.id)
        return pb2.VoteResult(term=res[0], result=res[1])

    def Suspend(self, request, context):
        client_commands(f'suspend {request.period}')
        suspend(request.period)
        return pb2.EmptyMessage()

    def GetLeader(self, request, context):
        client_commands('getleader')
        id, ip = get_leader()
        return pb2.LeaderInfo(id=id, address=ip)


server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
pb2_grpc.add_RaftServiceServicer_to_server(Handler(), server)
server.add_insecure_port(f'{ip}:{port}')
server.start()
timer_t = Thread(target=timer_thr)
lead_t = Thread(target=leader_job)
[t.start() for t in [timer_t, lead_t]]
try:
    server.wait_for_termination()
except KeyboardInterrupt:
    dead = True
    [t.join() for t in [timer_t, lead_t]]
    print("Shutdowned")
    sys.exit()
