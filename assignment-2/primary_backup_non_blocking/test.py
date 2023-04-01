import registry
import server
import client
import asyncio

import time
import uuid

if __name__ == "__main__":
    # TODO: a Run registry

    # TODO: b Run N replicas

    # c
    cl = client.Client()

    # d
    replicas = cl.get_replica_list()
    file_uuid1 = uuid.uuid4()
    asyncio.run(cl.write(
        filename="sample.txt",
        content="Hello, world",
        file_uuid=file_uuid1,
    ), debug=True)
    time.sleep(5)

    # e
    for rp in replicas:
        cl.read(
            file_uuid=file_uuid1,
            replica=rp
        )

    # f
    file_uuid2 = uuid.uuid4()
    asyncio.run(cl.write(
        filename="test.txt",
        content="Bye, world",
        file_uuid=file_uuid2,
    ), debug=True)
    time.sleep(5)

    # g
    for rp in replicas:
        cl.read(
            file_uuid=file_uuid2,
            replica=rp
        )

    # # h
    # cl.delete(file_uuid=file_uuid1, replica=replicas[0])
    # time.sleep(2)

    # # i
    # for rp in replicas:
    #     cl.read(
    #         file_uuid=file_uuid1,
    #         replica=rp
    #     )
