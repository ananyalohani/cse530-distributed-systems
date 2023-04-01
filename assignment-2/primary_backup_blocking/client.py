import uuid
import random

import pbb_pb2
import pbb_pb2_grpc
import grpc


class Client:
    REGISTRY_ADDRESS = "[::]:56149"
    replica_list = None

    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def read(self, file_uuid, replica=None):
        rp = replica or self.get_replica_for_request()
        with grpc.insecure_channel(rp) as channel:
            stub = pbb_pb2_grpc.ReplicaStub(channel)
            response = stub.Read(pbb_pb2.ReadRequest(uuid=str(file_uuid)))
            print(f"[.] Status message: {response.message}")
            print(f"    File: {response.filename or 'None'}")
            print(f"    Content: {response.content or 'None'}")
            print(f"    Version: {response.version or 'None'}\n")

    def write(self, filename, content, file_uuid, replica=None):
        rp = replica or self.get_replica_for_request()
        with grpc.insecure_channel(rp) as channel:
            stub = pbb_pb2_grpc.ReplicaStub(channel)
            response = stub.Write(
                pbb_pb2.WriteRequest(
                    filename=filename,
                    content=content,
                    uuid=str(file_uuid),
                )
            )
            print(f"[.] Status message: {response.message}")
            print(f"    UUID: {response.uuid or 'None'}")
            print(f"    Version: {response.version or 'None'}\n")

    def delete(self, file_uuid, replica=None):
        rp = replica or self.get_replica_for_request()
        with grpc.insecure_channel(rp) as channel:
            stub = pbb_pb2_grpc.ReplicaStub(channel)
            response = stub.Delete(pbb_pb2.DeleteRequest(uuid=str(file_uuid)))
            print(f"[.] Status message: {response.message}")
            print(f"    UUID: {file_uuid}\n")

    def get_replica_for_request(self):
        self.get_replica_list()
        replica = random.choice(self.replica_list)
        return replica

    def get_replica_list(self):
        with grpc.insecure_channel(self.REGISTRY_ADDRESS) as channel:
            stub = pbb_pb2_grpc.RegistryStub(channel)
            response = stub.GetReplicaList(
                pbb_pb2.GetReplicaListRequest(name=str(self.client_id), address=None)
            )
            self.replica_list = response.replicas
            return self.replica_list
