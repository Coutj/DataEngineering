import faker
from sqlalchemy import create_engine
from faker import Faker
import pandas as pd
from elasticsearch import Elasticsearch, helpers


def db_conn():

    conn_string = "postgresql://admin:admin@postgres:5000/dataengineering"
    engine =create_engine(conn_string)

    conn = engine.connect()

    return conn


def fake_df_users_postgres():

    fake = Faker()
    data = {'name': [], 'id': [], 'street': [], 'city': [], 'zip': []}


    for r in range(2, 1000):
        data['name'].append(fake.name())
        data['street'].append(fake.street_address())
        data['city'].append(fake.city())
        data['zip'].append(fake.zipcode())

        data['id'].append(r)

    df_users = pd.DataFrame(data)

    return df_users


def fake_df_users_es():

    fake = Faker()
    data = {'name': [], 'street': [], 'city': [], 'zip': []}


    for r in range(1, 1000):
        data['name'].append(fake.name())
        data['street'].append(fake.street_address())
        data['city'].append(fake.city())
        data['zip'].append(fake.zipcode())

    df_users = pd.DataFrame(data)

    return df_users

def insert_into_postgres():
    fake = Faker()

    conn = db_conn()
    df_users = fake_df_users_postgres()

    df_users.to_sql('users', if_exists='append',con=conn, index=False)


    conn.close()


def insert_into_elasticsearch():
    fake = Faker()

    es = Elasticsearch({'127.0.0.1'})

    # doc = {
    #     "name": fake.name(),
    #     "street": fake.street_address(),
    #     "city": fake.city(),
    #     "zip": fake.zipcode()
    # }

    # result = es.index(index="users", doc_type="doc", body=doc)
    
    actions = [
        {
            "_index": "users",
            "_type": "doc",
            "_source": {
                "name": fake.name(),
                "street": fake.street_address(),
                "city": fake.city(),
                "zip": fake.zipcode()
            }
        } for x in range(2, 1000)
    ]

    # print(fake_df_users_es().to_json(orient='records'))

    result = helpers.bulk(es, actions)
    
    print(result)


def query_elastic():
    es = Elasticsearch()

    doc = {
        "query":{
            "match":{"name": "Antonio*"}
        }
        
    }

    result = es.search(
        index="users",
        body=doc,
        size=10
    )

    print(result)
    print('\n\n')
    for doc in result['hits']['hits']:
        print(doc['_source'])

    df = pd.json_normalize(result['hits']['hits'])
    print(df)

if __name__ == '__main__':
    insert_into_postgres()
    # insert_into_elasticsearch()
    # query_elastic()