from concurrent import futures

import grpc

import abd_pb2
import abd_pb2_grpc

import time
from random import randrange
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("port", default=2222, type=str)
parser.add_argument("value", default=1, type=str)
args = parser.parse_args()


class ABDServer(abd_pb2_grpc.ABDServiceServicer):
    def read1(self, read1_req, context):
        print("Read1 receiver got: {}".format(read1_req))

        slp_time = randrange(30)
        print(slp_time)
        if slp_time > 25:
            return
        else:
            time.sleep(slp_time)
            print("Sending Respond")
            return abd_pb2.Read1Response(value=args.value, timestamp=int(args.value))

    def read2(self, read2_req, context):
        print("Read2 receiver got: {}".format(read2_req))

        slp_time = randrange(30)
        print(slp_time)

        if slp_time > 25:
            return
        else:
            time.sleep(slp_time)
            print("Sending Respond")
            return abd_pb2.AckResponse()

    def write(self, write_req, context):
        print("Write receiver got: {}".format(write_req))
        slp_time = randrange(30)
        print(slp_time)

        if slp_time > 25:
            return
        else:
            time.sleep(slp_time)
            print("Sending Respond")
            return abd_pb2.AckResponse()


print("Listening to {}".format(args.port))
server = grpc.server(futures.ThreadPoolExecutor())
abd_pb2_grpc.add_ABDServiceServicer_to_server(ABDServer(), server)
server.add_insecure_port("[::]:{}".format(args.port))
server.start()
while True:
    time.sleep(60)
