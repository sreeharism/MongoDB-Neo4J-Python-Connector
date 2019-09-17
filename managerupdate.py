import datetime
from mongotriggers import MongoTrigger
from pymongo import MongoClient
from neo4j import GraphDatabase


def get_neo4j_driver(hosturi="bolt://localhost:7687", username="neo4j", password="passme"):
    return GraphDatabase.driver(hosturi, auth=(username, password))


user_data_dict = {
    # insert/create
    'i': ['_id', 'profession', 'age', 'Country', 'state'],
    # update
    'u': ['_id', 'profession', 'age', 'Country', 'state'],
    # delete
    'd': ['_id']
}


def run_query(function, args):
    driver = get_neo4j_driver()
    with driver.session() as session:
        session.write_transaction(function, args)


def add_user(tx, query):
    tx.run(query)
    return True


def get_query_from_key_value(data_dict):
    query_part = "{"
    for key, value in data_dict.items():
        query_part += "{}: '{}', ".format(key, value)
    query_part = query_part.strip().rstrip(',') + '}'
    return query_part


def query_builder(operation, data, collection):
    query_part = get_query_from_key_value(data)
    if operation == 'i':
        query = "CREATE(n:{} {})".format("User", query_part)
    elif operation == 'u':
        query = """MATCH (n:User { _id: '%s' }),(p:User { _id: '%s' })
                MERGE (n)-[r:follows]->(p)"""
    elif operation == 'd':
        query = "MATCH (n:User { _id: '%s' }) DETACH DELETE n"%data['_id']
    else:
        query = ""
    return query


def notification_manager(data_object):
    global user_data_dict
    db_data = data_object['o']
    print(data_object)
    collection = data_object['ns'].replace('LawsD.', '')
    operation = data_object['op']
    if collection == 'Users':
        # do the user related stuffs here and all
        data = {}
        for key in user_data_dict[operation]:
            data[key] = db_data[key]
        query = query_builder(operation, data, collection)
        print(query)
        run_query(add_user, query)
    elif collection == "something else":
        pass


def get_mongo_client(host="localhost", port=27017):
    '''get pymongo client '''
    return MongoClient(host=host, port=port)


def start_tailoplog(triggers, db='LawsD', collection='Users'):
    # listens to update/insert/delete, any of these will trigger the callback
    triggers.register_op_trigger(notification_manager, 'LawsD', 'Users')
    triggers.tail_oplog()


def stop_tailoplog(trigger):
    print("stopping")
    triggers.stop_oplog()


if __name__ == '__main__':
    # driver = get_neo4j_driver()
    try:

        client = get_mongo_client()
        trigger = MongoTrigger(client)
        start_tailoplog(trigger)
    except KeyboardInterrupt as e:
        stop_tailoplog(trigger)
