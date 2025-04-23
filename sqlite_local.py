import sqlite3
from datetime import datetime, timedelta
import random

def create_in_memory_db():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # Create table
    cursor.execute('''
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            signup_date TEXT,
            age INTEGER,
            is_active BOOLEAN
        )
    ''')

    # Generate 50 fake customers
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jake"]
    domains = ["example.com", "mail.com", "test.org"]

    customers = []
    base_date = datetime(2022, 1, 1)
    for i in range(50):
        name = random.choice(first_names) + str(i)
        email = f"{name.lower()}@{random.choice(domains)}"
        signup_date = (base_date + timedelta(days=random.randint(0, 730))).strftime("%Y-%m-%d")
        age = random.randint(18, 65)
        is_active = random.choice([0, 1])  # SQLite uses 0/1 for boolean
        customers.append((name, email, signup_date, age, is_active))

    # Insert into the table
    cursor.executemany('''
        INSERT INTO customers (name, email, signup_date, age, is_active)
        VALUES (?, ?, ?, ?, ?)
    ''', customers)

    conn.commit()
    return conn
