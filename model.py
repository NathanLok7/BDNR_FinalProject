#!/usr/bin/env python3
import datetime
import json
import os
import model
import pydgraph
#from faker import Faker

#fake = Faker()

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')


def set_schema(client):
    schema = """
    type Customer {
        uid_customer
        age
        destination
        transport
        reason
        month
    }

    uid_customer: int @index(int) .
    age: int @index(int) .
    destination: string @index(term) .
    transport: string .
    reason: string .
    month: int .


    type Airport {
        airlines
        flight
        flights_per_month
        month
    }

    airlines string @index(term) .
    flights int .
    flights_per_month: int .
    month: int .

    """
    return client.alter(pydgraph.Operation(schema=schema))


def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)


def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


def close_client_stub(client_stub):
    client_stub.close()



def create_data(client):
    client_stub = create_client_stub()
    client = create_client(client_stub)
    model.set_schema(client)

    # Create a new transaction.
    txn = client.txn()
    try:
        p = {
                "uid": "_:cliente1",
                "nombre": "Juan",
                "edad": 30,
                "destino": "Miami",
                "transporte": "Carro",
                "razon": "Vacaciones",
                "fecha": datetime.now(),
            },
        "aeropuerto_data": [
            {
                "uid": "_:aeropuerto1",
                "aerolineas": "Volaris",
                "vuelos": 100,
                "vuelos_por_mes": 10,
                "fecha": datetime.now(),
            },
        ]
        data = []
        '''
        for i in range(1, 11):
            cliente_data = {
                "uid": f"_:cliente{i}",
                "nombre": fake.name(),
                "edad": fake.random_int(min=18, max=60),
                "destino": fake.city(),
                "transporte": fake.word(),
                "razon": fake.text(),
                "fecha": datetime.now(),
            }

            aeropuerto_data = {
                "uid": f"_:aeropuerto{i}",
                "aerolineas": fake.company(),
                "vuelos": fake.random_int(min=50, max=200),
                "vuelos_por_mes": fake.random_int(min=5, max=20),
                "fecha": datetime.now(),
            }'''
            #data.extend([cliente_data, aeropuerto_data])

        response = txn.mutate(set_obj=p)

        # Commit transaction.
        commit_response = txn.commit()
        print(f"Commit Response: {commit_response}")

        print(f"UIDs: {response.uids}")
    finally:
        # Clean up. 
        # Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()



def delete_customer(client, name):
    # Create a new transaction.
    txn = client.txn()
    try:
        query1 = '''
        query search_user($a: string) {
            all(func: eq(customer, $a)) {
                uid_customer
            }
        }
        '''
        variables1 = {'$a': name}
        res1 = client.txn(read_only=True).query(query1, variables=variables1)
        ppl1 = json.loads(res1.json)
        for person in ppl1['all']:
            print("UID: " + person['uid'])
            txn.mutate(del_obj=person)
            print(f"{name} deleted")
        commit_response = txn.commit()
        print(commit_response)
    finally:
        txn.discard()


def costumer_plus_18(client, name):
    query = """query search_user($a: string) {
        
    }"""

    variables = {'$a': name}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print(f"Number of people named {name}: {len(ppl['all'])}")
    print(f"Data associated with {name}:\n{json.dumps(ppl, indent=2)}")

def s(client, name):
    query = """query search_user_and_relations($a: string) {
        all(func: eq(username, $a)) {
            uid
            username
            subscribes {
                uid
                username
                num_of_subscribers
                videos_published
                directs_made
            }
            banned {
                uid
                banned_info
            }
            starts{
                uid
                direct_name
            }
            publish{
                uid
                video_name               
            }
        }
    }"""

    variables = {'$a': name}
    res = client.txn(read_only=True).query(query, variables=variables)
    result = json.loads(res.json)

    # Print results.
    print(f"All Data associated with {name}:\n{json.dumps(result, indent=2)}")

def search_customers_who_doesnt_drives(client, num):
    query = """
        query search_user_by_num_subscribers($num: int) {
            all(func: eq(num_of_subscribers, $num)) {
                uid
                username
                num_of_subscribers
            }
        }
    """

    variables = {'$num': num}
    res = client.txn(read_only=True).query(query, variables=variables)
    result = json.loads(res.json)

    print(f"All Users with this amount of subscribers {num}:\n{json.dumps(result, indent=2)}")


def customers_per_month(client, name):
    query = """query search_user_subscribed_by_reverse($a: string) {
        query {
          count(func: has(username)) 
        }
    }"""

    variables = {'$a': name}
    res = client.txn(read_only=True).query(query, variables=variables)
    result = json.loads(res.json)

    # Print results.
    print(f"All subscribers that subscribes {name}:\n{json.dumps(result, indent=2)}")

def count_query(client):
    query = """
        query {
          count(func: has(username)) 
        }
    """

    res = client.txn(read_only=True).query(query)
    result = json.loads(res.json)

    count = result['count'][0]

    print(f"Total number of nodes with the 'username' predicate: {count}")


def search_customers_18_walks_month(client):
    # Add new data.
    txn = client.txn()
    try:
        p = {
            "uid": "_:newuser",
            "dgraph.type": "Username",
            "username": "New User",
			"subscribers": 1,
            "starts": [
                {
                    "uid": "_:bestdirect",
                    "dgraph.type": "Direct",
                    "username": "New User",
                }
            ],
			"publish": [
                {
                    'uid': '_:videonew',
					'dgraph.type': 'Video',
					'video_name': 'Video nuevo del canal'
                }
            ]
        }

        response = txn.mutate(set_obj=p)

        # Commit transaction.
        commit_response = txn.commit()
        print(f"Commit Response: {commit_response}")

        print(f"UIDs: {response.uids}")
    finally:
        txn.discard()


def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))

