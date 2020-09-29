import psycopg2
import json
import sys

connection = psycopg2.connect(host='localhost', database='credit_cards', user='postgres', password='horses12')
cursor = connection.cursor()

def create_tables(connection):
    sql = """CREATE TABLE people
    (person_id varchar(100) PRIMARY KEY,
    first_name varchar(100),
    last_name varchar(100))"""

    sql2 = """CREATE TABLE credit_cards
    (person_id varchar(100),
    card_number varchar(100),
    expiration_date date,
    authorizor varchar(100),
    balance bigint,
    FOREIGN KEY (person_id) references people(person_id))"""

    cursor.execute(sql)
    cursor.execute(sql2)
    connection.commit()

#create_tables(connection)

file = open("bank_clients.json")
bank_clients = json.load(file)
file.close()

sql = """INSERT INTO people (person_id, first_name, last_name)
        VALUES  (%(id)s, %(first_name)s, %(last_name)s)"""
card_sql = """INSERT INTO credit_cards (person_id, card_number, expiration_date, authorizor, balance)
              VALUES (%(id)s, %(card_number)s, %(expiration_date)s, %(authorizer)s, %(balance)s)"""

for person in bank_clients:
    cursor.execute(sql, {'id': person['id'], 'first_name': person['first_name'], 'last_name': person['last_name']})
    for card in person['credit_cards']:
        cursor.execute(card_sql, {'id': person['id'], 'card_number': card['card_number'], 'expiration_date':card['expiration_date'], 'authorizer': card['authorizer'], 'balance': card['balance']})
connection.commit()









