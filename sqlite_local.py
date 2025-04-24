import sqlite3
from datetime import datetime, timedelta
import random
import faker

def create_in_memory_db():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    fake = faker.Faker()

    # --- Customers Table
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

    customers = []
    for _ in range(100):
        name = fake.first_name()
        email = fake.email()
        signup_date = fake.date_between(start_date="-3y", end_date="today").strftime("%Y-%m-%d")
        age = random.randint(18, 75)
        is_active = random.choice([0, 1])
        customers.append((name, email, signup_date, age, is_active))

    cursor.executemany(
        'INSERT INTO customers (name, email, signup_date, age, is_active) VALUES (?, ?, ?, ?, ?)',
        customers
    )

    # --- Orders Table
    cursor.execute('''
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            order_date TEXT,
            amount REAL,
            payment_method TEXT
        )
    ''')

    payment_methods = ["credit_card", "debit_card", "paypal", "pix", "crypto"]
    orders = []
    for _ in range(100):
        customer_id = random.randint(1, 100)
        order_date = fake.date_between(start_date="-2y", end_date="today").strftime("%Y-%m-%d")
        amount = round(random.uniform(10.0, 500.0), 2)
        payment_method = random.choice(payment_methods)
        orders.append((customer_id, order_date, amount, payment_method))

    cursor.executemany(
        'INSERT INTO orders (customer_id, order_date, amount, payment_method) VALUES (?, ?, ?, ?)',
        orders
    )

    # --- Support Tickets Table
    cursor.execute('''
        CREATE TABLE support_tickets (
            ticket_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            issue TEXT,
            created_at TEXT,
            resolved BOOLEAN
        )
    ''')

    issues = [
        "Login issue", "Payment failed", "Shipping delay", "Bug report",
        "Feature request", "Data mismatch", "Account locked", "Refund request"
    ]

    tickets = []
    for _ in range(100):
        customer_id = random.randint(1, 100)
        issue = random.choice(issues)
        created_at = fake.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d")
        resolved = random.choice([0, 1])
        tickets.append((customer_id, issue, created_at, resolved))

    cursor.executemany(
        'INSERT INTO support_tickets (customer_id, issue, created_at, resolved) VALUES (?, ?, ?, ?)',
        tickets
    )

    conn.commit()
    return conn
