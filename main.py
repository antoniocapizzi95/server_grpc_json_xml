import grpc
import xmltodict
import addressbook_pb2
import addressbook_pb2_grpc
from concurrent import futures
import utils
import logging
import json
import sys

def xml_parse(xml):
    return json.loads(json.dumps(xmltodict.parse(xml)))

def get_person(db, number):
    for item in db:
        for phone in item["phones"]:
            if phone["number"] == number["number"]:
                return item
    return None

def edit_person(db, new_person):
    edit = False
    index = 0
    for item in db:
        if item["id"] == int(new_person["id"]):
            edit = True
            break
        index = index + 1
    if type(new_person["phones"]) is not list:
        new_person["phones"] = [new_person["phones"]]
    if type(new_person["id"]) is not int:
        new_person["id"] = int(new_person["id"])
    db[index] = new_person
    return edit

class RPCServiceServicer(addressbook_pb2_grpc.RPCServiceServicer):
    def __init__(self):
        self.db = utils.read_route_guide_database()

    def GetPersonByPhoneNumber(self, request, context):
        if request.format == "json":
            person = get_person(self.db, json.loads(request.payload))
            if person is None:
                return addressbook_pb2.Message(format="json", payload="{}")
            else:
                response_object = addressbook_pb2.Message(format="json", payload=json.dumps(person))
                print("json size: " + str(sys.getsizeof(response_object.SerializeToString())))
                return response_object
        else:
            person = get_person(self.db, xml_parse(request.payload)["PhoneNumber"])
            if person is None:
                return addressbook_pb2.Message(format="xml", payload=xmltodict.unparse({}))
            else:
                response_object = addressbook_pb2.Message(format="xml", payload=xmltodict.unparse({"person": person}))
                print("xml size: " + str(sys.getsizeof(response_object.SerializeToString())))
                return response_object


    def EditPeople(self, request_iterator, context):
        req_format = ""
        for request in request_iterator:
            req_format = request.format
            payload = request.payload
            if req_format == "json":
                res = edit_person(self.db, json.loads(payload))
                if not res:
                    return addressbook_pb2.Message(format="json", payload=json.dumps({"result": False}))
            else:
                res = edit_person(self.db, xml_parse(payload)["Person"])
                if not res:
                    return addressbook_pb2.Message(format="xml", payload=xmltodict.unparse({"result": False}))

        if req_format == "json":
            return addressbook_pb2.Message(format="json", payload=json.dumps({"result": True}))
        else:
            return addressbook_pb2.Message(format="xml", payload=xmltodict.unparse({"result": True}))

    def ListPeopleByPhoneType(self, request, context):
        for item in self.db:
            for phone in item["phones"]:
                if request.format == "json":
                    payload = json.loads(request.payload)
                    if phone["kind"] == payload["kind"]:
                        yield addressbook_pb2.Message(format="json", payload=json.dumps(item))
                else:
                    payload = xml_parse(request.payload)["PhoneNumber"]
                    if phone["kind"] == int(payload["kind"]):
                        yield addressbook_pb2.Message(format="xml", payload=xmltodict.unparse({"people": item}))

    def GetPeopleById(self, request_iterator, context):
        people = []
        for request in request_iterator:
            for item in self.db:
                if request.format == "json":
                    payload = json.loads(request.payload)
                    if item["id"] == payload["id"]:
                        people.append(item)
                        yield addressbook_pb2.Message(format="json", payload=json.dumps(item))
                else:
                    payload = xml_parse(request.payload)["RequestId"]
                    if item["id"] == int(payload["id"]):
                        people.append(item)
                        yield addressbook_pb2.Message(format="xml", payload=xmltodict.unparse({"people": item}))

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    addressbook_pb2_grpc.add_RPCServiceServicer_to_server(
        RPCServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()