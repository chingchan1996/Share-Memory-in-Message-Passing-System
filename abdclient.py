import grpc

import abd_pb2
import abd_pb2_grpc
import argparse
from math import ceil
import time
import threading

parser = argparse.ArgumentParser()
parser.add_argument("host_port", default=None, type=str)
parser.add_argument("action", default=None, type=str)
parser.add_argument("register", default=1, type=str)
parser.add_argument("value", default=None, type=str)

args = parser.parse_args()


class Function(object):

    def __init__(self, _host_port_list, _reg, _value=None):
        self.host_port_list = _host_port_list
        self.acks = {hp: None for hp in self.host_port_list}
        self.mojarity = int(len(self.host_port_list)/2 + 1) if len(self.host_port_list)%2 == 0 else ceil(len(self.host_port_list)/2) #mod

        self.acks_lock = False

        self.reg = _reg
        self.value = _value

    def execute(self):
        print("Not Implemented")

    def invoke(self, _hp):
        print("Not Implemented")

    def getTime(self):
        return int(time.time())

    def isMojarityAchieved(self):
        counter = 0
        self.acquireLock()
        for v in self.acks.values():
            if v is not None and v != 'X':
                counter += 1
        self.releaseLock()
        print("the counter:{} the mojarity:{}".format(counter, self.mojarity))
        self.printACKs()
        return counter >= self.mojarity

    def isFinished(self):
        return None not in self.acks.values()

    def acquireLock(self):
        self.acks_lock = True

    def releaseLock(self):
        self.acks_lock = False

    def printACKs(self):
        print(self.acks)

class Writer(Function):

    def execute(self):

        #phase 1
        threads = {}
        for hp in self.host_port_list:
            threads[hp] = threading.Thread(target=self.invoke, args=(hp,))
            threads[hp].start()

        while not self.isFinished():
            time.sleep(1)

        if self.isMojarityAchieved():
            print('Write Successes')
        else:
            print('Write Fails')

    def invoke(self, hp):

        ts = self.getTime()

        print("opening channel to {}".format(hp))
        channel = grpc.insecure_channel(hp)
        print("creating stub to {}".format(hp))
        stubs = abd_pb2_grpc.ABDServiceStub(channel)
        print("making W request to {} with ts:{}".format(hp, ts))

        try:
            response = stubs.write(abd_pb2.WriteRequest(register=self.reg, timestampe=ts, value=self.value))
            while self.acks_lock:
                 time.sleep(1)

            self.acquireLock()
            self.acks[hp] = 'ACK'
            self.releaseLock()
            print('{}: responds with {}'.format(hp, 'ACK'))
        except:
            response = 'X'
            self.acquireLock()
            self.acks[hp] = response
            self.releaseLock()
            print('{}: responds with {}'.format(hp, response))

class Reader(Function):

    def execute(self):
        # phase 1
        threads = {}
        for hp in self.host_port_list:
            threads[hp] = threading.Thread(target=self.invoke, args=(hp,))
            threads[hp].start()

        while not self.isFinished():
            time.sleep(1)

        if self.isMojarityAchieved():
            print('Read P1 Successes')
        else:
            print('Read P1 Fails, terminating')
            return

        # Analyze
        self.readValue()


        # phase 2
        self.acks = {hp: None for hp in self.host_port_list}


        threads = {}
        for hp in self.host_port_list:
            threads[hp] = threading.Thread(target=self.invoke_2, args=(hp,))
            threads[hp].start()

        while not self.isFinished():
            time.sleep(1)

        if self.isMojarityAchieved():
            print('Read P2 Successes, the value is {} with timestamp:{}'.format(self.value, self.max_ts))
        else:
            print('Read P2 Fails, terminating')
            return



    def invoke(self, hp):
        print("opening channel to {}".format(hp))
        channel = grpc.insecure_channel(hp)
        print("creating stub to {}".format(hp))
        stubs = abd_pb2_grpc.ABDServiceStub(channel)
        print("making R1 request to {}".format(hp))
        try:
            response = stubs.read1(abd_pb2.Read1Request(register=self.reg))

            while self.acks_lock:
                time.sleep(1)

            self.acquireLock()
            self.acks[hp] = response
            self.releaseLock()
            print('{}: responds with {}'.format(hp, self.acks[hp])) #MOD

        except:
            response = 'X'
            self.acquireLock()
            self.acks[hp] = response
            self.releaseLock()
            print('{}: responds with {}'.format(hp, self.acks[hp])) #MOD


    def readValue(self):
        self.max_ts = -1

        for v in self.acks.values():

            if isinstance(v, str):
                continue

            if self.max_ts == -1:
                self.max_ts = v.timestamp
                self.value = v.value

            if v.timestamp > self.max_ts:
                self.max_ts = v.timestamp
                self.value = v.value

    def invoke_2(self, hp):
        print("opening channel to {}".format(hp))
        channel = grpc.insecure_channel(hp)
        print("creating stub to {}".format(hp))
        stubs = abd_pb2_grpc.ABDServiceStub(channel)
        print("making R2 request to {}".format(hp))
        try:
            response = stubs.read2(abd_pb2.Read2Request(register=self.reg, timestamp=self.max_ts, value=self.value))#mod

            while self.acks_lock:
                time.sleep(1)

            self.acquireLock()
            self.acks[hp] = 'ACK'
            self.releaseLock()
            print('{}: responds with {}'.format(hp, self.acks[hp])) #mod

        except:
            response = 'X'
            self.acquireLock()
            self.acks[hp] = response
            self.releaseLock()
            print('{}: responds with {}'.format(hp, response))


if __name__ == '__main__':
    test = False

    target_list = args.host_port.split(',')
    action_type = args.action
    register_num = args.register
    value = args.value

    if action_type == "write":
        obj = Writer(target_list, register_num, _value=value)
    else:
        obj = Reader(target_list, register_num)

    obj.execute()

