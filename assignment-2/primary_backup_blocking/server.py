import socket
from concurrent import futures
from nanoid import generate
from urllib.parse import unquote
import time
import os

import pbb_pb2
import pbb_pb2_grpc
import grpc

MAX_LENGTH = 1000

# ! TODO: TEST THE CODE!


class ReplicaServicer(pbb_pb2_grpc.ReplicaServicer):
    id = None
    primary = {
        'address': None,
        'port': None
    }
    replica_list = None
    datastore = {}

    def __init__(self, address, port, registry_address):
        self.id = generate(size=10)
        print(
            f"[.] Registering replica at {address}:{port} with registry @ {registry_address}...")
        with grpc.insecure_channel(registry_address) as channel:
            stub = pbb_pb2_grpc.RegistryStub(channel)
            response = stub.Register(
                pbb_pb2.RegisterRequest(
                    id=self.id,
                    address=address,
                    port=port
                )
            )
            if response.status == pbb_pb2.Status.ERROR:
                print(f"[*] {response.message}")
                print("FAIL")
                return
            self.primary['address'] = response.replica.address
            self.primary['port'] = response.replica.port
            if response.replica.address == address and response.replica.port == port:
                self.replica_list = []
                return
            print(f"[.] {response.message}")
            print("SUCCESS")

    def Read(self, request, context):
        print(
            f"[.] Read request received from client {unquote(context.peer())}")
        if request.uuid not in self.datastore:
            return pbb_pb2.ReadResponse(
                status=pbb_pb2.Status.ERROR, content="File does not exist."
            )
        file = self.datastore[request.uuid]
        path = os.path.join(f"replica_{id}", file[0])
        try:
            fo = open(path, "r")
            content = fo.read(MAX_LENGTH)
        except FileNotFoundError:
            return pbb_pb2.ReadResponse(
                status=pbb_pb2.Status.ERROR, content="File already deleted", timestamp=file[1]
            )
        return pbb_pb2.ReadResponse(status=pbb_pb2.Status.OK, name=fo.name, content=content, timestamp=file[1])

    def Write(self, request, context):
        print(
            f"[.] Write request received from client {unquote(context.peer())}")
        path = os.path.join(os.getcwd(), f"replica_{id}")
        files = os.listdir(path)
        if request.uuid not in self.datastore and request.name in files:
            return pbb_pb2.WriteResponse(
                status=pbb_pb2.Status.ERROR, message="File with the same name already exists."
            )
        if request.uuid in self.datastore and request.name in files:
            return pbb_pb2.WriteResponse(
                status=pbb_pb2.Status.ERROR, message="Deleted file cannot be updated."
            )
        timestamp = None
        if request.uuid not in self.datastore:
            timestamp = time.time()
            self.datastore[request.uuid] = (request.name, timestamp)
        path = os.path.join(path, request.name)
        fo = open(path, "w")
        fo.write(request.content)
        # TODO: Send WRITE update to all replicas or primary
        return pbb_pb2.WriteResponse(status=pbb_pb2.Status.OK, message="SUCCESS", timestamp=timestamp or self.datastore[request.uuid][1], uuid=request.uuid)

    def Delete(self, request, context):
        print(
            f"[.] Delete request received from client {unquote(context.peer())}")
        if request.uuid not in self.datastore:
            return pbb_pb2.BaseResponse(
                status=pbb_pb2.Status.ERROR, message="File does not exist."
            )
        path = os.path.join(os.getcwd(), f"replica_{id}")
        files = os.listdir(path)
        name = self.datastore[request.uuid][0]
        if name not in files:
            return pbb_pb2.BaseResponse(
                status=pbb_pb2.Status.ERROR, message="File already deleted."
            )
        os.remove(os.path.join(path, name))
        # TODO: Send DELETE update to all replicas or primary
        return pbb_pb2.BaseResponse(status=pbb_pb2.Status.OK, message="SUCCESS")

    def InformPrimary(self, request, context):
        print(
            f"[.] Inform request received from registry {unquote(context.peer())}")
        replicas = request.replicas
        self.replica_list.extend(replicas)
        return pbb_pb2.BaseResponse(status=pbb_pb2.Status.OK, message="Replica list updated.")
