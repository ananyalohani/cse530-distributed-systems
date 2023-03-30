import registry
import server
import client

import uuid

if __name__ == "__main__":
    # TODO: a Run registry

    # TODO: b Run N replicas

    # c
    cl = client.Client()

    # e
    replicas = cl.get_replica_list()

    # d
    file_uuid1 = uuid.uuid4()
    cl.write(
        filename="sample.txt",
        content="Hello, world",
        file_uuid=file_uuid1,
        replica=replicas[0]
    )

    # for rp in replicas:
    cl.read(
        file_uuid=file_uuid1,
        replica=replicas[0]
    )

    # f
    file_uuid2 = uuid.uuid4()
    cl.write(
        filename="test.txt",
        content="Bye, world",
        file_uuid=file_uuid2,
        replica=replicas[0]
    )

    # g
    # for rp in replicas:
    cl.read(
        file_uuid=file_uuid2,
        replica=replicas[0]
    )

    # h
    cl.delete(file_uuid=file_uuid1, replica=replicas[0])

    # i
    # for rp in replicas:
    cl.read(
        file_uuid=file_uuid1,
        replica=replicas[0]
    )
