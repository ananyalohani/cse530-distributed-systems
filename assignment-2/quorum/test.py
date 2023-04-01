import registry
import server
import client

import time
import uuid

if __name__ == "__main__":
    # TODO: a Run registry

    # TODO: b Run N replicas

    # c
    cl = client.Client()

    # d
    file_uuid1 = uuid.uuid4()
    cl.write(
        filename="sample.txt",
        content="Hello, world",
        file_uuid=file_uuid1,
    )
    time.sleep(2)

    # e
    cl.read(file_uuid=file_uuid1)

    # f
    file_uuid2 = uuid.uuid4()
    cl.write(
        filename="test.txt",
        content="Bye, world",
        file_uuid=file_uuid2,
    )
    time.sleep(2)

    # g
    cl.read(file_uuid=file_uuid2)

    # h
    cl.delete(file_uuid=file_uuid1)
    time.sleep(2)

    # i
    cl.read(file_uuid=file_uuid1)
