# Import the necessary libraries
import pandas as pd  # Used for data processing and analysis
import sqlite3  # Used for interacting with SQLite databases

# Read customer and order data from CSV files
customers_df = pd.read_csv('customer.csv')  # Load customer data into a DataFrame from a CSV file
orders_df = pd.read_csv('orders.csv')  # Load order data into a DataFrame from a CSV file

# Merge the order and customer data
merged_df = pd.merge(orders_df, customers_df, on='CustomerID', how='inner')  # Perform an inner join on the two DataFrames based on the CustomerID column

# Calculate the total amount for each order
merged_df['TotalAmount'] = merged_df['Quantity'] * merged_df['Price']  # Create a new column TotalAmount by multiplying Quantity and Price

# Set the order status based on the order date
merged_df['Status'] = merged_df['OrderDate'].apply(lambda d: 'New' if d.startswith('2025-03') else 'Old')  # Create a new column Status, marking orders with dates starting with "2025-03" as "New" and others as "Old"

# Filter high-value orders
high_value_orders = merged_df[merged_df['TotalAmount'] > 5000]  # Select orders where the TotalAmount is greater than 5000

# Connect to an SQLite database
conn = sqlite3.connect('ecommerce.db')  # Create or connect to an SQLite database named ecommerce.db

# Create a table to store high-value order data
create_table_query = '''
CREATE TABLE IF NOT EXISTS HighValueOrders (
    OrderID INTEGER,
    CustomerID INTEGER,
    Name TEXT,
    Email TEXT,
    Product TEXT,
    Quantity INTEGER,
    Price REAL,
    OrderDate TEXT,
    TotalAmount REAL,
    Status TEXT
)
'''
conn.execute(create_table_query)  # Execute the SQL statement to create the table if it does not exist

# Write the high-value order data to the database
high_value_orders.to_sql('HighValueOrders', conn, if_exists='replace', index=False)  # Write the DataFrame data to the database table, replacing it if it already exists

# Query the high-value order data from the database
result = conn.execute('SELECT * FROM HighValueOrders')  # Execute an SQL query to retrieve all data from the table
for row in result.fetchall():  # Iterate through the query results
    print(row)  # Print each record

# Close the database connection
conn.close()

# Print completion message
print("ETL process completed successfully!")