import socket
import threading
from concurrent import futures
from urllib.parse import unquote
import time
import os

import pbn_pb2
import pbn_pb2_grpc
import grpc

MAX_LENGTH = 1000


class ReplicaServicer(pbn_pb2_grpc.ReplicaServicer):
    REGISTRY_ADDRESS = "[::]:8888"

    id = None
    address = None
    primary = None
    replica_list = None
    datastore = {}
    is_primary = False

    def __init__(self, address):
        self.address = address
        print(
            f"[.] Registering replica at {address} with registry @ {self.REGISTRY_ADDRESS}..."
        )
        with grpc.insecure_channel(self.REGISTRY_ADDRESS) as channel:
            stub = pbn_pb2_grpc.RegistryStub(channel)
            response = stub.Register(
                pbn_pb2.RegisterRequest(
                    address=address,
                )
            )
            if response.status == pbn_pb2.Status.ERROR:
                print(f"[*] {response.message}")
                return
            self.id = response.replica_id
            self.primary = response.primary_address
            if response.primary_address == address:
                self.is_primary = True
                self.replica_list = []
            print(f"[.] {response.message}")

    def Read(self, request, context):
        from_address = request.from_address
        print(
            f"[.] READ request received from {from_address}")
        if request.uuid not in self.datastore:
            return pbn_pb2.ReadResponse(
                status=pbn_pb2.Status.ERROR,
                message="READ FAILURE: File does not exist.",
            )
        file = self.datastore[request.uuid]
        path = os.path.join(f"data/replica_{self.id}", file[0])
        try:
            fo = open(path, "r")
            content = fo.read(MAX_LENGTH)
        except FileNotFoundError:
            return pbn_pb2.ReadResponse(
                status=pbn_pb2.Status.ERROR,
                message="READ FAILURE: File already deleted",
                version=file[1],
            )
        return pbn_pb2.ReadResponse(
            status=pbn_pb2.Status.OK,
            message="READ SUCCESS",
            filename=fo.name,
            content=content,
            version=file[1],
        )

    def write_to_replica(self, request):
        path = os.path.join(os.getcwd(), f"data/replica_{self.id}")
        files = os.listdir(path)
        if request.uuid not in self.datastore and request.filename in files:
            return pbn_pb2.WriteResponse(
                status=pbn_pb2.Status.ERROR,
                message="WRITE FAILURE: File with the same name already exists.",
            )
        if request.uuid in self.datastore and request.filename not in files:
            return pbn_pb2.WriteResponse(
                status=pbn_pb2.Status.ERROR,
                message="WRITE FAILURE: Deleted file cannot be updated.",
            )
        self.datastore[request.uuid] = (request.filename, request.version)
        path = os.path.join(path, request.filename)
        fo = open(path, "w")
        fo.write(request.content)
        fo.close()
        return pbn_pb2.WriteResponse(
            status=pbn_pb2.Status.OK,
            message="WRITE SUCCESS",
            version=request.version or self.datastore[request.uuid][1],
            uuid=request.uuid,
        )

    def send_write_request_to_backups(self, request):
        replica_list = list(
            filter(lambda x: x != self.address, self.replica_list))
        response = None
        time.sleep(2)
        for rp in replica_list:
            with grpc.insecure_channel(rp) as channel:
                stub = pbn_pb2_grpc.ReplicaStub(channel)
                response = stub.Write(request)
                response.message = f"REPLICA {response.message}"
                if response.status == pbn_pb2.Status.ERROR:
                    return response
        return response

    def Write(self, request, context):
        from_address = request.from_address
        print(f"[.] WRITE request received from {from_address}")
        if self.is_primary:
            request.version = int(time.time())
            thread = threading.Thread(
                target=self.send_write_request_to_backups,
                args=(
                    pbn_pb2.WriteRequest(
                        filename=request.filename,
                        content=request.content,
                        uuid=request.uuid,
                        version=request.version,
                        from_address=self.address,
                    ),
                ),
            )
            primary_response = self.write_to_replica(request)
            thread.start()
            return primary_response
        else:
            if from_address == self.primary:
                return self.write_to_replica(request)
            else:
                with grpc.insecure_channel(self.primary) as channel:
                    stub = pbn_pb2_grpc.ReplicaStub(channel)
                    response = stub.Write(
                        pbn_pb2.WriteRequest(
                            filename=request.filename,
                            content=request.content,
                            uuid=request.uuid,
                            from_address=self.address,
                        )
                    )
                    return response

    def delete_from_replica(self, request):
        if request.uuid not in self.datastore:
            return pbn_pb2.BaseResponse(
                status=pbn_pb2.Status.ERROR,
                message="DELETE FAILURE: File does not exist.",
            )
        path = os.path.join(os.getcwd(), f"data/replica_{self.id}")
        files = os.listdir(path)
        name = self.datastore[request.uuid][0]
        self.datastore[request.uuid] = (name, request.version)
        if name not in files:
            return pbn_pb2.BaseResponse(
                status=pbn_pb2.Status.ERROR,
                message="DELETE FAILURE: File already deleted.",
            )
        os.remove(os.path.join(path, name))
        return pbn_pb2.BaseResponse(status=pbn_pb2.Status.OK, message="DELETE SUCCESS")

    def send_delete_request_to_backups(self, request):
        replica_list = list(
            filter(lambda x: x != self.address, self.replica_list))
        response = None
        time.sleep(2)
        for rp in replica_list:
            with grpc.insecure_channel(rp) as channel:
                stub = pbn_pb2_grpc.ReplicaStub(channel)
                response = stub.Delete(request)
                response.message = f"REPLICA {response.message}"
                if response.status == pbn_pb2.Status.ERROR:
                    return response
        return response

    def Delete(self, request, context):
        from_address = request.from_address
        print(f"[.] DELETE request received from {from_address}")
        if self.is_primary:
            request.version = int(time.time())
            thread = threading.Thread(
                target=self.send_delete_request_to_backups,
                args=(
                    pbn_pb2.DeleteRequest(
                        uuid=request.uuid,
                        version=request.version,
                        from_address=self.address,
                    ),
                ),
            )
            primary_response = self.delete_from_replica(request)
            thread.start()
            return primary_response
        else:
            if from_address == self.primary:
                return self.delete_from_replica(request)
            else:
                with grpc.insecure_channel(self.primary) as channel:
                    stub = pbn_pb2_grpc.ReplicaStub(channel)
                    response = stub.Delete(
                        pbn_pb2.DeleteRequest(
                            uuid=request.uuid,
                            from_address=self.address,
                        )
                    )
                    return response

    def InformPrimary(self, request, context):
        print(
            f"[.] INFORM request received from registry {self.REGISTRY_ADDRESS}")
        replica = request.replica_address
        if self.address == self.primary:
            self.replica_list.append(replica)
        return pbn_pb2.BaseResponse(
            status=pbn_pb2.Status.OK, message="Replica list updated BY PRIMARY"
        )


def serve():
    port = 0
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        port = s.getsockname()[1]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    server_address = f"[::]:{port}"
    pbn_pb2_grpc.add_ReplicaServicer_to_server(
        ReplicaServicer(
            server_address,
        ),
        server,
    )
    server.add_insecure_port(server_address)
    server.start()
    print(f"[.] Server node started on {server_address}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
