import socket
from concurrent import futures
from nanoid import generate
from urllib.parse import unquote
import time
import os
import sys

import pbb_pb2
import pbb_pb2_grpc
import grpc

MAX_LENGTH = 1000

# ! TODO: TEST THE CODE!


class ReplicaServicer(pbb_pb2_grpc.ReplicaServicer):
    id = None
    address = None
    REGISTRY_ADDRESS = "[::]:56149"
    primary = None
    replica_list = None
    datastore = {}
    is_primary = False

    def __init__(self, address):
        self.address = address
        print(
            f"[.] Registering replica at {address} with registry @ {self.REGISTRY_ADDRESS}...")
        with grpc.insecure_channel(self.REGISTRY_ADDRESS) as channel:
            stub = pbb_pb2_grpc.RegistryStub(channel)
            response = stub.Register(
                pbb_pb2.RegisterRequest(
                    address=address,
                )
            )
            if response.status == pbb_pb2.Status.ERROR:
                print(f"[*] {response.message}")
                return
            self.id = response.replica_id
            self.primary = response.primary_address
            if response.primary_address == address:
                self.is_primary = True
                self.replica_list = []
            print(f"[.] {response.message}")

    def Read(self, request, context):
        print(
            f"[.] Read request received from client {unquote(context.peer())}")
        if request.uuid not in self.datastore:
            return pbb_pb2.ReadResponse(
                status=pbb_pb2.Status.ERROR,
                message="READ FAILURE: File does not exist."
            )
        file = self.datastore[request.uuid]
        path = os.path.join(f"data/replica_{self.id}", file[0])
        try:
            fo = open(path, "r")
            content = fo.read(MAX_LENGTH)
        except FileNotFoundError:
            return pbb_pb2.ReadResponse(
                status=pbb_pb2.Status.ERROR,
                message="READ FAILURE: File already deleted",
                version=file[1]
            )
        return pbb_pb2.ReadResponse(
            status=pbb_pb2.Status.OK,
            message="READ SUCCESS",
            filename=fo.name,
            content=content,
            version=file[1]
        )

    def Write(self, request, context):
        print(
            f"[.] Write request received from client {unquote(context.peer())}")
        path = os.path.join(os.getcwd(), f"data/replica_{self.id}")
        files = os.listdir(path)
        if request.uuid not in self.datastore and request.filename in files:
            return pbb_pb2.WriteResponse(
                status=pbb_pb2.Status.ERROR,
                message="WRITE FAILURE: File with the same name already exists."
            )
        if request.uuid in self.datastore and request.filename in files:
            return pbb_pb2.WriteResponse(
                status=pbb_pb2.Status.ERROR,
                message="WRITE FAILURE: Deleted file cannot be updated."
            )
        timestamp = None
        if request.uuid not in self.datastore:
            timestamp = time.time()
            self.datastore[request.uuid] = (request.filename, int(timestamp))
        path = os.path.join(path, request.filename)
        fo = open(path, "w")
        fo.write(request.content)
        # TODO: Send WRITE update to all replicas or primary
        return pbb_pb2.WriteResponse(
            status=pbb_pb2.Status.OK,
            message="WRITE SUCCESS",
            version=int(timestamp) or self.datastore[request.uuid][1],
            uuid=request.uuid
        )

    def Delete(self, request, context):
        print(
            f"[.] Delete request received from client {unquote(context.peer())}")
        if request.uuid not in self.datastore:
            return pbb_pb2.BaseResponse(
                status=pbb_pb2.Status.ERROR, message="DELETE FAILURE: File does not exist."
            )
        path = os.path.join(os.getcwd(), f"data/replica_{self.id}")
        files = os.listdir(path)
        name = self.datastore[request.uuid][0]
        if name not in files:
            return pbb_pb2.BaseResponse(
                status=pbb_pb2.Status.ERROR,
                message="DELETE FAILURE: File already deleted."
            )
        os.remove(os.path.join(path, name))
        # TODO: Send DELETE update to all replicas or primary
        return pbb_pb2.BaseResponse(
            status=pbb_pb2.Status.OK,
            message="DELETE SUCCESS"
        )

    def InformPrimary(self, request, context):
        print(
            f"[.] Inform request received from registry {self.REGISTRY_ADDRESS}")
        replica = request.replica_address
        if self.address == self.primary:
            self.replica_list.append(replica)
        return pbb_pb2.BaseResponse(
            status=pbb_pb2.Status.OK,
            message="Replica list updated."
        )


def serve():
    port = 0
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        port = s.getsockname()[1]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    server_address = f"[::]:{port}"
    pbb_pb2_grpc.add_ReplicaServicer_to_server(
        ReplicaServicer(
            server_address,
        ),
        server
    )
    server.add_insecure_port(server_address)
    server.start()
    print(f"[.] Server node started on {server_address}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
