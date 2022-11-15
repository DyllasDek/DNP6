import grpc
import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2

channel = None
stub = None


def connect(ip):
    global channel
    global stub
    try:
        channel = grpc.insecure_channel(ip)
        stub = pb2_grpc.RaftServiceStub(channel)
        stub.CheckAlive(pb2.EmptyMessage())
    except grpc.RpcError as rpc_error:
        if rpc_error.code() == grpc.StatusCode.UNAVAILABLE:
            print(f'The server {ip} is unavailable')
            channel = None
            stub = None
        else:
            print("Something wrong with grpc, try again.")
        pass


def get_leader():
    resp = stub.GetLeader(pb2.EmptyMessage())
    print(f'{resp.id} {resp.address}')


def suspend(period):
    stub.Suspend(pb2.Time(period=period))


if __name__ == "__main__":
    while True:
        try:
            line = input("> ")
            if len(line) != 0:
                line = line.split(' ', 1)
                if line[0] == "connect":
                    line = line[1].split(' ', 1)
                    connect(f"{line[0]}:{line[1]}")
                    continue
                if line[0] == "getleader":
                    get_leader()
                    continue
                if line[0] == 'suspend':
                    suspend(int(line[1]))
                if line[0] == 'quit':
                    break
        except KeyboardInterrupt:
            break
        # except:
        #    print("Something wrong, try again!")
        #    pass

print("The client ends")
