import sys
import grpc
import chord_pb2_grpc as pb2_grpc
import chord_pb2 as pb2

channel = None
stub = None


def send_messages(val):
    for n in list(map(int, val)):
        req = pb2.PrimeMessage(
            num=n)
        yield req


def connect(ip):
    global channel
    global stub
    try:
        channel = grpc.insecure_channel(ip)
        stub = pb2_grpc.SimpleServiceStub(channel)
    except grpc.RpcError as rpc_error:
        if rpc_error.code() == grpc.StatusCode.UNAVAILABLE:
            print(f'The server {ip} is unavailable')
        else:
            print("Something wrong with grpc, try again.")
        pass


def getleader():
    #resp = stub.GetLeader(pb2.GetInfo())
    #print(f'{resp.id} {resp.ip}')
    print('lox1')


def suspend(period):
    #resp = stub.GetChord(pb2.GetInfo())

    # for elem in resp.table:
    #    print(elem)
    print('lox2')


if __name__ == "__main__":
    while True:
        try:
            line = input("> ")
            if len(line) != 0:
                line = line.split(' ', 1)
                if line[0] == "connect":
                    connect(line[1])
                    continue

                if line[0] == "getleader":
                    getleader()
                    continue
                if line[0] == 'suspend':
                    time = line[1].split(' ', 1)
                    suspend(time)
                if line[0] == 'quit':
                    break
        except KeyboardInterrupt:
            break
        except:
            print("Something wrong, try again!")
            pass

print("Shutting down")
