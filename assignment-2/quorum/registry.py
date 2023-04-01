import socket
from urllib.parse import unquote
from concurrent import futures
import os
import random

import pbb_pb2
import pbb_pb2_grpc
import grpc


class RegistryServicer(pbb_pb2_grpc.RegistryServicer):
    replica_list = []
    quorum_data = {
        "N": 0,
        "N_R": 0,
        "N_W": 0,
    }

    def is_quorum_satisfied(self):
        return (
            (self.quorum_data["N"] < self.quorum_data["N_R"] + self.quorum_data["N_W"])
            and (0 < self.quorum_data["N_R"] <= self.quorum_data["N"])
            and (0 < self.quorum_data["N_W"] <= self.quorum_data["N"])
        )

    def __init__(self):
        while not self.is_quorum_satisfied():
            self.quorum_data["N"] = int(input("Number of replicas: "))
            self.quorum_data["N_R"] = int(input("Read quorum size: "))
            self.quorum_data["N_W"] = int(input("Write quorum size: "))
            if not self.is_quorum_satisfied():
                print("Ensure N < N_R + N_W, and 0 < N_R, N_W <= N.")

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
        return pbb_pb2.RegisterResponse(
            status=pbb_pb2.Status.OK,
            message=f"Server {address} registered.",
            replica_id=replica_id,
        )

    def GetReplicaList(self, request, context):
        quorum_size_map = {
            "READ": self.quorum_data["N_R"],
            "WRITE": self.quorum_data["N_W"],
            "ALL": self.quorum_data["N"],
        }
        replicas = self.replica_list
        if request.purpose is not None:
            quorum_size = quorum_size_map[request.purpose]
            replicas = random.sample(self.replica_list, quorum_size)
        return pbb_pb2.GetReplicaListResponse(replicas=replicas)


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
