import psycopg2
import sys

connection = psycopg2.connect(host='localhost', database='credit_cards', user='postgres', password='horses12')
cursor = connection.cursor()

def print_usage():
    print(f"""
    python {sys.argv[0]} [-balanceGreaterThan <gt>] | [-balanceLessThan <lt] | [-overDrawn] | [-expired] | [-fields f1, f2, f3]
    OPTIONS:
        -balanceGreaterThan <gt>    Retrieve all people having any card with a balance greater than 'gt'
        -balanceLessThan <lt>       Retrieve all people having any card with a balance less than 'lt'
        -overdrawn                  Same as -balanceLessThan 0
        -expired                    Retrieve all people having any card that has expired
        -fields f1, f2, f3          Only include fields f1, f2, f3 in the results. If omitted, all fields are output.
                                    Possible fields include: 
                                        Person: id, first_name, last_name, email, home_time_zone
                                        Card: authorizer, card_number, expiration_date, balance
    NOTE:   Multiple options may be used together. For example using both the -expired and -overdrawn options will 
            retrieve all people with any card expired and any card overdrawn (not necessarily the same card).
""")

greater_than = None
less_than = None
expired = None
person_fields = ["first_name", 'last_name', 'home_time_zone'] #... and more (all) select *
card_fields = ['authorizer', 'card_number', 'expiration_date', 'balance']
fields = person_fields + card_fields
select_clause  = ""
where_clause = ""

try:
    for index, arg in enumerate(sys.argv): #sets it up like (i = 0, i++...)
        if arg == "-balanceGreaterThan":
            greater_than = sys.argv[index + 1]
            where_clause += f" AND balance > {greater_than}"
        elif arg == "-balanceLessThan":
            less_than = sys.argv[index + 1]
            where_clause += f" AND balance < {less_than}"
        elif arg == "-overDrawn":
            less_than = 0
            where_clause += " AND balance < 0"
        elif arg == "-expired":
            expired = True
            where_clause += " AND expiration_date < now()"
        elif arg == "-fields":
            fields = sys.argv[index + 1]
            select_clause = fields

    if select_clause == "":
        select_clause = "*"

    sql_query = f"Select {select_clause} from credit_cards c JOIN people p on c.person_id = p.person_id where card_number is not null {where_clause} " \

    cursor.execute(sql_query)
    rows = cursor.fetchall()
    for row in rows:
         print(str(row))

    cursor.close()
except Exception as ex:
    print(ex)