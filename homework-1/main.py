"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import os
import csv

# files
customers_data_file = "north_data/customers_data.csv"
employees_data_file = "north_data/employees_data.csv"
orders_data_file = "north_data/orders_data.csv"

# connection
conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password=os.getenv("PSQL_KEY")
)


def upload_customers(path):
    with open(path, "r") as file:
        reader = csv.reader(file)
        next(reader)
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s)",
                reader
            )
        conn.commit()


def upload_employees(path):
    with open(path, "r") as file:
        reader = csv.reader(file)
        next(reader)
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                reader
            )
        conn.commit()


def upload_orders(path):
    with open(path, "r") as file:
        reader = csv.reader(file)
        next(reader)
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city) "
                "VALUES (%s, %s, %s, %s, %s)",
                reader
            )
        conn.commit()


if __name__ == "__main__":
    upload_customers(path=customers_data_file)
    upload_employees(path=employees_data_file)
    upload_orders(path=orders_data_file)
