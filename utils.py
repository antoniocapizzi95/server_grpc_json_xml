import json

import addressbook_pb2

def get_phones_number(person_id):
    phones_list = []
    with open("phones_db.json") as phones_db_file:
        for item in json.load(phones_db_file):
            if item['user_id'] == person_id:
                phone = {"number": item['number'], "kind": item['type']}
                phones_list.append(phone)
    return phones_list


def read_route_guide_database():

    person_list = []
    with open("person_db.json") as person_db_file:
        for item in json.load(person_db_file):
            phones = get_phones_number(item['id'])
            person = {"id": item['id'], "name": item['name'], "email": item['email'], "phones": phones}
            person_list.append(person)
    return person_list