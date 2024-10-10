import sqlite3
import random
import string
import os
import time
import shutil

# Set up the initial test database
db_name = '/mnt/data/test_project_db.sqlite'
conn = sqlite3.connect(db_name)
cursor = conn.cursor()

# Create sample tables for the project
cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    price REAL NOT NULL
)
''')

# Function to generate random strings
def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Populate tables with test data
num_users = 10000  # Number of users to create
num_products = 1000  # Number of products to create
num_orders = 1000000  # Number of orders to create

# Insert users
cursor.executemany('INSERT INTO users (username, email) VALUES (?, ?)', 
                   [(random_string(), f"{random_string()}@example.com") for _ in range(num_users)])

# Insert products
cursor.executemany('INSERT INTO products (name, description, price) VALUES (?, ?, ?)', 
                   [(random_string(), "Sample product description", random.uniform(1.0, 100.0)) for _ in range(num_products)])

# Insert orders
order_data = [
    (random.randint(1, num_users), random.randint(1, num_products), random.uniform(1.0, 100.0)) 
    for _ in range(num_orders)
]
cursor.executemany('INSERT INTO orders (user_id, product_id, amount) VALUES (?, ?, ?)', order_data)

# Commit and close the initial setup
conn.commit()
conn.close()

# Creating directory for backups if it doesn't exist
backup_dir = '/mnt/data/backups/'
os.makedirs(backup_dir, exist_ok=True)

# Function to perform full backup
def full_backup(src_db, backup_path):
    shutil.copyfile(src_db, backup_path)

# Timing full backup
full_backup_path = os.path.join(backup_dir, 'full_backup.sqlite')
start_time = time.time()
full_backup(db_name, full_backup_path)
full_backup_time = time.time() - start_time

# Getting the size of the full backup
full_backup_size = os.path.getsize(full_backup_path)

print("Backup Type", "Full")
print("Backup Time (s)", full_backup_time)
print("Backup Size (bytes)", full_backup_size)

def incremental_backup(src_db, backup_path, last_backup_path):
    """Perform an incremental backup by copying the changed data since the last backup."""
    conn_src = sqlite3.connect(src_db)
    conn_backup = sqlite3.connect(backup_path)
    cursor_src = conn_src.cursor()
    cursor_backup = conn_backup.cursor()

    # Attach the previous full backup as the reference database
    cursor_backup.execute('ATTACH DATABASE ? AS last_backup', (last_backup_path,))

    # Create the new incremental backup tables if they don't exist
    cursor_backup.execute('CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, email TEXT, created_at TIMESTAMP)')
    cursor_backup.execute('CREATE TABLE IF NOT EXISTS orders (order_id INTEGER PRIMARY KEY, user_id INTEGER, product_id INTEGER, amount REAL, order_date TIMESTAMP)')
    cursor_backup.execute('CREATE TABLE IF NOT EXISTS products (product_id INTEGER PRIMARY KEY, name TEXT, description TEXT, price REAL)')

    # Back up only changed users (newly added or updated email addresses)
    cursor_backup.execute('''
    INSERT INTO users SELECT * FROM main.users
    WHERE user_id NOT IN (SELECT user_id FROM last_backup.users)
    OR email != (SELECT email FROM last_backup.users WHERE last_backup.users.user_id = main.users.user_id)
    ''')

    # Back up only new orders
    cursor_backup.execute('''
    INSERT INTO orders SELECT * FROM main.orders
    WHERE order_id NOT IN (SELECT order_id FROM last_backup.orders)
    ''')

    # No need to back up the products table in this simulation as it hasn't changed
    # Commit and close connections
    conn_backup.commit()
    conn_src.close()
    conn_backup.close()

# Retry the incremental backup using the adjusted approach
incremental_backup_path = os.path.join(backup_dir, 'incremental_backup_v2.sqlite')
start_time = time.time()
incremental_backup(db_name, incremental_backup_path, full_backup_path)
incremental_backup_time = time.time() - start_time

# Getting the size of the new incremental backup
incremental_backup_size = os.path.getsize(incremental_backup_path)

print("Backup Type", "Incremental")
print("Backup Time (s)", incremental_backup_time)
print("Backup Size (bytes)", incremental_backup_size)

# Further adjustment for the reverse delta backup approach to correctly access the columns

def reverse_delta_backup(src_db, backup_path, current_full_backup_path):
    """Perform a reverse delta backup by storing changes that allow reconstruction of older versions."""
    conn_src = sqlite3.connect(src_db)
    conn_backup = sqlite3.connect(backup_path)
    cursor_src = conn_src.cursor()
    cursor_backup = conn_backup.cursor()

    # Attach the current full backup as a reference database
    cursor_src.execute('ATTACH DATABASE ? AS current_full_backup', (current_full_backup_path,))

    # Create tables to store reverse deltas for changes in the users and orders tables
    cursor_backup.execute('CREATE TABLE IF NOT EXISTS users_delta (user_id INTEGER PRIMARY KEY, old_email TEXT)')
    cursor_backup.execute('CREATE TABLE IF NOT EXISTS orders_delta (order_id INTEGER PRIMARY KEY, old_amount REAL)')

    # Store reverse deltas for users where email has changed
    cursor_src.execute('''
    SELECT main.users.user_id, current_full_backup.users.email
    FROM main.users
    INNER JOIN current_full_backup.users ON main.users.user_id = current_full_backup.users.user_id
    WHERE main.users.email != current_full_backup.users.email
    ''')
    users_deltas = cursor_src.fetchall()
    cursor_backup.executemany('INSERT INTO users_delta (user_id, old_email) VALUES (?, ?)', users_deltas)

    # Store reverse deltas for orders where the amount has changed
    cursor_src.execute('''
    SELECT main.orders.order_id, current_full_backup.orders.amount
    FROM main.orders
    INNER JOIN current_full_backup.orders ON main.orders.order_id = current_full_backup.orders.order_id
    WHERE main.orders.amount != current_full_backup.orders.amount
    ''')
    orders_deltas = cursor_src.fetchall()
    cursor_backup.executemany('INSERT INTO orders_delta (order_id, old_amount) VALUES (?, ?)', orders_deltas)

    # Commit changes and close the connections
    conn_backup.commit()
    conn_src.close()
    conn_backup.close()

# Perform reverse delta backup using the corrected approach
reverse_delta_backup_path = os.path.join(backup_dir, 'reverse_delta_backup_v4.sqlite')
start_time = time.time()
reverse_delta_backup(db_name, reverse_delta_backup_path, full_backup_path)
reverse_delta_backup_time = time.time() - start_time

# Get the size of the reverse delta backup
reverse_delta_backup_size = os.path.getsize(reverse_delta_backup_path)

print("Backup Type", "Reverse Delta")
print("Backup Time (s)", reverse_delta_backup_time)
print("Backup Size (bytes)", reverse_delta_backup_size)

