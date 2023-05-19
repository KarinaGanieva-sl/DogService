import psycopg2
from datetime import datetime


def get_connection():
    conn = psycopg2.connect(database="postgres",
                            user="student",
                            password="HSDStoTestDb3711",
                            host="database-1.czcdhgn8biyx.us-east-1.rds.amazonaws.com",
                            port="5432")
    return conn


def create_scheme():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS DogService")
    conn.commit()


def drop_scheme():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DROP SCHEMA IF EXISTS DogService")
    conn.commit()


def create_tables():
    commands = ('''CREATE TABLE IF NOT EXISTS DogService.Customer(
                    id SERIAL PRIMARY KEY,
                    name varchar,
                    balance real,
                    address varchar
                )''',
                '''CREATE TABLE IF NOT EXISTS DogService.Branch(
                    id serial PRIMARY KEY,
                    name varchar unique ,
                    address varchar
                )''',
                '''CREATE TABLE IF NOT EXISTS DogService.Employee(
                            id SERIAL PRIMARY KEY,
                            name varchar,
                            position varchar,
                            salary real,
                            branch_id bigint,
                            FOREIGN KEY(branch_id) REFERENCES DogService.Branch(id))
                 ''',
                '''
               CREATE TABLE IF NOT EXISTS DogService.Procedure(
                        id SERIAL PRIMARY KEY,
                        name varchar,
                        branch_id bigint,
                        price real,
                        toolkit varchar,
                        FOREIGN KEY(branch_id) REFERENCES DogService.Branch(id))
               ''',
                '''
               CREATE TABLE IF NOT EXISTS DogService.Dog(
                        id SERIAL PRIMARY KEY,
                        name varchar,
                        owner_id bigint,
                        age real,
                        breed varchar,
                        FOREIGN KEY(owner_id) REFERENCES DogService.Customer(id))
               ''',
                '''
               CREATE TABLE IF NOT EXISTS DogService.Appointment(
                        id SERIAL PRIMARY KEY,
                        branch_id bigint,
                        datetime timestamp,
                        dog_id bigint,
                        FOREIGN KEY(branch_id) REFERENCES DogService.Branch(id),
                        FOREIGN KEY(dog_id) REFERENCES DogService.Dog(id)
                        )
               ''',
                '''
               CREATE TABLE IF NOT EXISTS DogService.AppointmentProcedure(
                        id SERIAL PRIMARY KEY,
                        appointment_id bigint,
                        procedure_id bigint,
                        FOREIGN KEY(appointment_id) REFERENCES DogService.Appointment(id),
                        FOREIGN KEY(procedure_id) REFERENCES DogService.Procedure(id)
                        )
               ''',
                '''
              CREATE TABLE IF NOT EXISTS DogService.Message(
                       id SERIAL PRIMARY KEY,
                       customer_id bigint,
                       datetime timestamp,
                       text varchar,
                       employee_id bigint,
                       FOREIGN KEY(customer_id) REFERENCES DogService.Customer(id),
                       FOREIGN KEY(employee_id) REFERENCES DogService.Employee(id)
                       )
              '''
                )
    conn = get_connection()
    cursor = conn.cursor()
    for command in commands:
        cursor.execute(command)
        conn.commit()


def insert_into_customer(customer):
    command = 'INSERT INTO dogservice.customer (name, balance, address) values (%s, %s, %s)'
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(command, customer)
    conn.commit()


def insert_into_branch(branch):
    command = 'INSERT INTO dogservice.branch (name, address) values (%s, %s)'
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(command, branch)
    conn.commit()


def insert_into_dog(dog):
    command = 'INSERT INTO dogservice.dog (name, owner_id, age, breed) values (%s, %s, %s, %s)'
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(command, dog)
    conn.commit()


def insert_into_procedure(procedure):
    command = 'INSERT INTO dogservice.procedure (name, branch_id, price, toolkit) values (%s, %s, %s, %s)'
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(command, procedure)
    conn.commit()


def insert_into_appointment(appointment):
    command = 'INSERT INTO dogservice.appointment (dog_id, branch_id, datetime) values (%s, %s, %s) returning id'
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(command, appointment)
    id = cursor.fetchone()[0]
    conn.commit()
    return id


def insert_into_appointment_procedure(appointment_procedure):
    command = 'INSERT INTO dogservice.appointmentprocedure (appointment_id, procedure_id) values (%s, %s)'
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(command, appointment_procedure)
    conn.commit()


def insert_into_tables():
    customers = [('Mike', 100, 300), ('Alice', 50, 833), ('Fernando', 74.5, 865.9), ('Kate', 55, 100)]
    for customer in customers:
        insert_into_customer(customer)
    dogs = [('yuki', 1, 2, 'retriever'), ('hurma', 2, 1, 'retriever')]
    for dog in dogs:
        insert_into_dog(dog)
    branches = [('WarmHands diagonal', 'Barcelona, carrer de arago 89'), ('WarmHands rambla', 'Barcelona, la rambla 23')]
    for branch in branches:
        insert_into_branch(branch)
    procedures = [('washing', 1, 100, ''), ('haircut', 1, 200,  ''), ('washing', 2, 80, ''), ('haircut', 2, 170, '')]
    for procedure in procedures:
        insert_into_procedure(procedure)


def create_appointment(appointment):
    # dog_id, branch_id, datetime, procedures
    appointment_id = insert_into_appointment(appointment[:-1])
    for procedure_id in appointment[3]:
        insert_into_appointment_procedure([appointment_id, procedure_id])


if __name__ == '__main__':
    # create_scheme()
    # create_tables()
    # insert_into_tables()
    # # dog_id, branch_id, datetime, procedures
    print('creating appointment')
    create_appointment([1, 1, datetime.now(), [2, 3]])
    print('success')
