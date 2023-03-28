import socket
from urllib.parse import unquote
from concurrent import futures

import pbb_pb2
import pbb_pb2_grpc
import grpc


class RegistryServicer(pbb_pb2_grpc.RegistryServicer):
    replica_list = []
    primary = {
        'address': None,
        'port': None
    }

    def Register(self, request, context):
        address = request.address
        port = request.port
        print(f"[.] REGISTER REQUEST FROM {address}:{port}")
        if any(s.address == address and s.port == port for s in self.replica_list):
            return pbb_pb2.RegisterResponse(
                status=pbb_pb2.Status.ERROR,
                message=f"Server '{address}:{port}' already registered.",
                replica=None
            )
        self.replica_list.append(
            pbb_pb2.ServerInfo(address=address, port=port))
        if self.primary['address'] is None:
            self.primary['address'] = address
            self.primary['port'] = port
            return pbb_pb2.RegisterResponse(
                status=pbb_pb2.Status.OK,
                message=f"Server '{address}:{port}' registered as primary.",
                replica=self.primary
            )
        # TODO: inform primary about new replica
        return pbb_pb2.RegisterResponse(
            status=pbb_pb2.Status.OK,
            message=f"Server '{address}:{port}' registered.",
            replica=self.primary
        )

    def GetReplicaList(self, request, context):
        print(
            f"[.] REPLICA LIST REQUEST FROM {request.address}:{unquote(context.peer())}")
        return pbb_pb2.GetReplicaListResponse(replicas=self.replica_list)


def serve():
    port = 0
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        port = s.getsockname()[1]
    registry_address = f"[::]:{port}"
    registry = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pbb_pb2_grpc.add_RegistryServicer_to_server(RegistryServicer(), registry)
    registry.add_insecure_port(registry_address)
    registry.start()
    print(f"[.] Registry node started on {registry_address}")
    registry.wait_for_termination()


if __name__ == "__main__":
    serve()
