import socket
from urllib.parse import unquote
from concurrent import futures
import os

import pbb_pb2
import pbb_pb2_grpc
import grpc


class RegistryServicer(pbb_pb2_grpc.RegistryServicer):
    replica_list = []
    primary = None

    def Register(self, request, context):
        address = request.address
        print(f"[.] REGISTER REQUEST FROM {address}")
        if any(r_address == address for r_address in self.replica_list):
            return pbb_pb2.RegisterResponse(
                status=pbb_pb2.Status.ERROR,
                message=f"Server {address} already registered.",
            )
        self.replica_list.append(address)
        replica_id = len(self.replica_list)
        path = os.path.join(os.getcwd(), f"data/replica_{replica_id}")
        if not os.path.exists(path):
            os.mkdir(path)
        if not self.primary:
            self.primary = address
            return pbb_pb2.RegisterResponse(
                status=pbb_pb2.Status.OK,
                message=f"Server {address} registered as primary.",
                primary_address=address,
                replica_id=replica_id,
            )
        if address != self.primary:
            with grpc.insecure_channel(self.primary) as channel:
                stub = pbb_pb2_grpc.ReplicaStub(channel)
                response = stub.InformPrimary(
                    pbb_pb2.InformPrimaryRequest(
                        replica_address=address,
                    )
                )
                print(f"[.] {response.message}")
        return pbb_pb2.RegisterResponse(
            status=pbb_pb2.Status.OK,
            message=f"Server {address} registered.",
            primary_address=self.primary,
            replica_id=replica_id,
        )

    def GetReplicaList(self, request, context):
        print(f"[.] REPLICA LIST REQUEST FROM CLIENT {request.name}")
        return pbb_pb2.GetReplicaListResponse(replicas=self.replica_list)


def serve():
    PORT = 56149
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", PORT))
    registry_address = f"[::]:{PORT}"
    registry = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pbb_pb2_grpc.add_RegistryServicer_to_server(RegistryServicer(), registry)
    registry.add_insecure_port(registry_address)
    registry.start()
    print(f"[.] Registry node started on {registry_address}")
    registry.wait_for_termination()


if __name__ == "__main__":
    serve()
