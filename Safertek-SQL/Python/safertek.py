import mysql.connector

def create_tables(cur):
    # Create Customers table
    cur.execute('''CREATE TABLE IF NOT EXISTS Customers (
                    CustomerID INT PRIMARY KEY,
                    FirstName VARCHAR(255),
                    LastName VARCHAR(255),
                    Email VARCHAR(255),
                    DateOfBirth DATE
                )''')

    # Create Products table
    cur.execute('''CREATE TABLE IF NOT EXISTS Products (
                    ProductID INT PRIMARY KEY,
                    ProductName VARCHAR(255),
                    Price DECIMAL(10, 2)
                )''')

    # Create Orders table
    cur.execute('''CREATE TABLE IF NOT EXISTS Orders (
                    OrderID INT PRIMARY KEY,
                    CustomerID INT,
                    OrderDate DATE,
                    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
                )''')

    # Create OrderItems table
    cur.execute('''CREATE TABLE IF NOT EXISTS OrderItems (
                    OrderItemID INT PRIMARY KEY,
                    OrderID INT,
                    ProductID INT,
                    Quantity INT,
                    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
                    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
                )''')

def insert_sample_data(cur):
    # Insert data into Customers table
    customers = [
        (1, 'John', 'Doe', 'john.doe@example.com', '1985-01-15'),
        (2, 'Jane', 'Smith', 'jane.smith@example.com', '1990-06-20')
    ]
    cur.executemany('''INSERT INTO Customers (CustomerID, FirstName, LastName, Email, DateOfBirth)
                       VALUES (%s, %s, %s, %s, %s)''', customers)

    # Insert data into Products table
    products = [
        (1, 'Laptop', 1000),
        (2, 'Smartphone', 600),
        (3, 'Headphones', 100)
    ]
    cur.executemany('''INSERT INTO Products (ProductID, ProductName, Price)
                       VALUES (%s, %s, %s)''', products)

    # Insert data into Orders table
    orders = [
        (1, 1, '2023-01-10'),
        (2, 2, '2023-01-12')
    ]
    cur.executemany('''INSERT INTO Orders (OrderID, CustomerID, OrderDate)
                       VALUES (%s, %s, %s)''', orders)

    # Insert data into OrderItems table
    order_items = [
        (1, 1, 1, 1),
        (2, 1, 3, 2),
        (3, 2, 2, 1),
        (4, 2, 3, 1)
    ]
    cur.executemany('''INSERT INTO OrderItems (OrderItemID, OrderID, ProductID, Quantity)
                       VALUES (%s, %s, %s, %s)''', order_items)

def list_all_customers(cur):
    cur.execute("SELECT * FROM Customers")
    for row in cur.fetchall():
        print(row)

def find_orders_in_january(cur):
    cur.execute("SELECT * FROM Orders WHERE OrderDate BETWEEN '2023-01-01' AND '2023-01-31'")
    for row in cur.fetchall():
        print(row)

def get_order_details(cur):
    cur.execute('''SELECT o.OrderID, c.FirstName, c.LastName, c.Email, o.OrderDate
                   FROM Orders o
                   JOIN Customers c ON o.CustomerID = c.CustomerID''')
    for row in cur.fetchall():
        print(row)

def list_products_in_order(cur, order_id):
    cur.execute('''SELECT p.ProductName, p.Price, oi.Quantity
                   FROM OrderItems oi
                   JOIN Products p ON oi.ProductID = p.ProductID
                   WHERE oi.OrderID = %s''', (order_id,))
    for row in cur.fetchall():
        print(row)

def calculate_total_spent_by_customers(cur):
    cur.execute('''SELECT c.CustomerID, c.FirstName, c.LastName, SUM(p.Price * oi.Quantity) AS TotalSpent
                   FROM Customers c
                   JOIN Orders o ON c.CustomerID = o.CustomerID
                   JOIN OrderItems oi ON o.OrderID = oi.OrderID
                   JOIN Products p ON oi.ProductID = p.ProductID
                   GROUP BY c.CustomerID''')
    for row in cur.fetchall():
        print(row)

def find_most_popular_product(cur):
    cur.execute('''SELECT p.ProductName, SUM(oi.Quantity) AS TotalOrdered
                   FROM Products p
                   JOIN OrderItems oi ON p.ProductID = oi.ProductID
                   GROUP BY p.ProductName
                   ORDER BY TotalOrdered DESC
                   LIMIT 1''')
    print(cur.fetchone())

def get_monthly_sales(cur):
    cur.execute('''SELECT MONTH(OrderDate) AS Month, COUNT(*) AS TotalOrders, SUM(p.Price * oi.Quantity) AS TotalSales
                   FROM Orders o
                   JOIN OrderItems oi ON o.OrderID = oi.OrderID
                   JOIN Products p ON oi.ProductID = p.ProductID
                   WHERE YEAR(OrderDate) = 2023
                   GROUP BY MONTH(OrderDate)''')
    for row in cur.fetchall():
        print(row)

def find_high_spending_customers(cur):
    cur.execute('''SELECT c.CustomerID, c.FirstName, c.LastName, SUM(p.Price * oi.Quantity) AS TotalSpent
                   FROM Customers c
                   JOIN Orders o ON c.CustomerID = o.CustomerID
                   JOIN OrderItems oi ON o.OrderID = oi.OrderID
                   JOIN Products p ON oi.ProductID = p.ProductID
                   GROUP BY c.CustomerID
                   HAVING TotalSpent > 1000''')
    for row in cur.fetchall():
        print(row)

def main():
    try:
        # Establish connection to MySQL database
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="vachaspathi",
            database="safertek",
            auth_plugin='mysql_native_password'
        )
        cur = conn.cursor()

        # Create tables and insert sample data
        create_tables(cur)
        insert_sample_data(cur)
        conn.commit()
        print("Sample data inserted successfully!\n")

        # Execute sample queries
        print("1. List all customers:")
        list_all_customers(cur)
        print("\n2. Find all orders placed in January 2023:")
        find_orders_in_january(cur)
        print("\n3. Get the details of each order, including the customer name and email:")
        get_order_details(cur)
        print("\n4. List the products purchased in a specific order (OrderID = 1):")
        list_products_in_order(cur, 1)
        print("\n5. Calculate the total amount spent by each customer:")
        calculate_total_spent_by_customers(cur)
        print("\n6. Find the most popular product (the one that has been ordered the most):")
        find_most_popular_product(cur)
        print("\n7. Get the total number of orders and the total sales amount for each month in 2023:")
        get_monthly_sales(cur)
        print("\n8. Find customers who have spent more than $1000:")
        find_high_spending_customers(cur)

    except mysql.connector.Error as err:
        print("MySQL Error:", err)

    finally:
        # Close the cursor and connection
        if 'cur' in locals() and cur is not None:
            cur.close()
        if 'conn' in locals() and conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
