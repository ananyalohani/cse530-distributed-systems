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

    def check_valid_response(self, response, final_response):
        is_valid = not final_response
        if not is_valid:
            try:
                is_valid = response.version > final_response.version
            except AttributeError:
                pass
        return is_valid

    def read(self, file_uuid):
        replicas = self.get_replica_list("READ")
        responses = []
        for rp in replicas:
            with grpc.insecure_channel(rp) as channel:
                stub = pbb_pb2_grpc.ReplicaStub(channel)
                response = stub.Read(pbb_pb2.ReadRequest(uuid=str(file_uuid)))
                responses.append(response)
        final_response = None
        for response in responses:
            if self.check_valid_response(response, final_response):
                final_response = response
        if final_response is None or final_response.status == pbb_pb2.Status.ERROR:
            print("[.] Status message: READ FAILURE")
            if final_response is not None:
                print(f"    Message: {final_response.message}")
            return
        print(f"[.] Status message: {final_response.message}")
        print(f"    File: {final_response.filename or 'None'}")
        print(f"    Content: {final_response.content or 'None'}")
        print(f"    Version: {final_response.version or 'None'}\n")

    def write(self, filename, content, file_uuid):
        replicas = self.get_replica_list("WRITE")
        responses = []
        for rp in replicas:
            with grpc.insecure_channel(rp) as channel:
                stub = pbb_pb2_grpc.ReplicaStub(channel)
                response = stub.Write(
                    pbb_pb2.WriteRequest(
                        filename=filename,
                        content=content,
                        uuid=str(file_uuid),
                    )
                )
                responses.append(response)
        final_response = None
        error_response = None
        for response in responses:
            if self.check_valid_response(response, final_response):
                final_response = response
                if response.status == pbb_pb2.Status.ERROR:
                    error_response = response
        if error_response is not None:
            print(f"[.] Status message: {error_response.message}")
            return
        if final_response is None:
            print("[.] Status message: WRITE FAILURE")
            return
        print(f"[.] Status message: {final_response.message}")
        print(f"    UUID: {final_response.uuid or 'None'}")
        print(f"    Version: {final_response.version or 'None'}\n")

    def delete(self, file_uuid):
        replicas = self.get_replica_list("WRITE")
        responses = []
        for rp in replicas:
            with grpc.insecure_channel(rp) as channel:
                stub = pbb_pb2_grpc.ReplicaStub(channel)
                response = stub.Delete(pbb_pb2.DeleteRequest(uuid=str(file_uuid)))
                responses.append(response)
        final_response = None
        for response in responses:
            if self.check_valid_response(response, final_response):
                final_response = response
        if final_response is None:
            print("[.] Status message: DELETE FAILURE")
            return
        print(f"[.] Status message: {final_response.message}")
        print(f"    UUID: {file_uuid}\n")

    def get_replica_list(self, purpose="ALL"):
        with grpc.insecure_channel(self.REGISTRY_ADDRESS) as channel:
            stub = pbb_pb2_grpc.RegistryStub(channel)
            response = stub.GetReplicaList(
                pbb_pb2.GetReplicaListRequest(name=self.client_id, purpose=purpose)
            )
            self.replica_list = response.replicas
            return self.replica_list
