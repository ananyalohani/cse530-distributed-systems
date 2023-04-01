import uuid
import random

import pbn_pb2
import pbn_pb2_grpc
import grpc


class Client:
    REGISTRY_ADDRESS = "[::]:8888"
    replica_list = None

    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def read(self, file_uuid, replica=None):
        rp = replica or self.get_replica_for_request()
        with grpc.insecure_channel(rp) as channel:
            stub = pbn_pb2_grpc.ReplicaStub(channel)
            response = stub.Read(
                pbn_pb2.ReadRequest(
                    uuid=str(file_uuid),
                    from_address=self.client_id,
                )
            )
            print(f"[.] Status message: {response.message}")
            print(f"    File: {response.filename or 'None'}")
            print(f"    Content: {response.content or 'None'}")
            print(f"    Version: {response.version or 'None'}\n")

    def write(self, filename, content, file_uuid, replica=None):
        rp = replica or self.get_replica_for_request()
        with grpc.insecure_channel(rp) as channel:
            stub = pbn_pb2_grpc.ReplicaStub(channel)
            response = stub.Write(
                pbn_pb2.WriteRequest(
                    filename=filename,
                    content=content,
                    uuid=str(file_uuid),
                    from_address=self.client_id,
                )
            )
            print(f"[.] Status message: {response.message}")
            print(f"    UUID: {response.uuid}")
            print(f"    Version: {response.version or 'None'}\n")

    def delete(self, file_uuid, replica=None):
        rp = replica or self.get_replica_for_request()
        with grpc.insecure_channel(rp) as channel:
            stub = pbn_pb2_grpc.ReplicaStub(channel)
            response = stub.Delete(
                pbn_pb2.DeleteRequest(
                    uuid=str(file_uuid),
                    from_address=self.client_id
                )
            )
            print(f"[.] Status message: {response.message}")
            print(f"    UUID: {file_uuid}\n")

    def get_replica_for_request(self):
        self.get_replica_list()
        replica = random.choice(self.replica_list)
        return replica

    def get_replica_list(self):
        with grpc.insecure_channel(self.REGISTRY_ADDRESS) as channel:
            stub = pbn_pb2_grpc.RegistryStub(channel)
            response = stub.GetReplicaList(
                pbn_pb2.GetReplicaListRequest(
                    name=str(self.client_id), address=None)
            )
            self.replica_list = response.replicas
            return self.replica_list


if __name__ == "__main__":
    # c
    cl = Client()

    # d
    replicas = cl.get_replica_list()
    file_uuid1 = uuid.uuid4()
    cl.write(
        filename="sample.txt",
        content="Hello, world",
        file_uuid=file_uuid1,
    )

    # e
    for rp in replicas:
        cl.read(
            file_uuid=file_uuid1,
            replica=rp
        )

    # f
    file_uuid2 = uuid.uuid4()
    cl.write(
        filename="test.txt",
        content="Bye, world",
        file_uuid=file_uuid2,
    )

    # g
    for rp in replicas:
        cl.read(
            file_uuid=file_uuid2,
            replica=rp
        )

    # h
    cl.delete(file_uuid=file_uuid1, replica=replicas[0])

    # i
    for rp in replicas:
        cl.read(
            file_uuid=file_uuid1,
            replica=rp
        )
