#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import mysql.connector
from mysql.connector import Error
from mysql.connector.pooling import MySQLConnectionPool

# Define the database configurations
dbconfig = {
    "host": "localhost",
    "user": "root",
    "password": "gingkoSQL12#$",
    "database": "little_lemon_db"
}

# Create a connection pool
pool_b = MySQLConnectionPool(
    pool_name="pool_b",
    pool_size=2,
    **dbconfig
)

# Test the connection
try:
    conn = pool_b.get_connection()
    print("Connected successfully!")
    conn.close()
except Error as e:
    print(f"Error: {e}")

    
    # Define the data to be inserted
guest1 = (8, "Anees", "Java", "18:00", 6)
guest2 = (5, "Bald", "Vin", "19:00", 6)
guest3 = (12, "Jay", "Kon", "19:30", 6)

# Get connections from the pool
conn1 = pool_b.get_connection()
conn2 = pool_b.get_connection()
conn3 = pool_b.get_connection()

# Create cursor objects
cursor1 = conn1.cursor()
cursor2 = conn2.cursor()
cursor3 = conn3.cursor()

# Define SQL query
insert_guests = "INSERT INTO Bookings (TableNumber, FirstName, LastName, BookingSlot, EmployeeID) VALUES (%s, %s, %s, %s, %s)"

# Execute query for each guest
cursor1.execute(insert_guests, guest1)
cursor1.execute(insert_guests, guest2)
cursor2.execute(insert_guests, guest3)

# Commit changes
conn1.commit()
conn2.commit()
conn3.commit()

# Return connections to the pool
cursor1.close()
cursor2.close()
cursor3.close()
conn1.close()
conn2.close()
conn3.close()


try:
    conn3 = pool_b.get_connection()
    cursor3 = conn3.cursor()
    cursor3.execute(insert_guests, guest3)
    conn3.commit()
    cursor3.close()
    conn3.close()
except mysql.connector.errors.PoolError as e:
    print(f"Error: {e}")

    
    # Task 3

# The name and EmployeeID of the Little Lemon manager
name = """
SELECT Name, EmployeeID
FROM Employee
WHERE Role = 'Manager'
"""
cursor.execute(name)
result = cursor.fetchone()
print("Little Lemon manager:")
print("Name:", result[0])
print("EmployeeID:", result[1])
print()

# The name and role of the employee who receives the highest salary
role = """
SELECT e.Name, s.Role
FROM Employee e
JOIN Salary s ON e.EmployeeID = s.EmployeeID
WHERE s.Salary = (SELECT MAX(Salary) FROM Salary)
"""
cursor.execute(role)
result = cursor.fetchone()
print("Employee with highest salary:")
print("Name:", result[0])
print("Role:", result[1])
print()

# The number of guests booked between 18:00 and 20:00
number_guests = """
SELECT COUNT(*)
FROM Bookings
WHERE HOUR(BookingSlot) BETWEEN 18 AND 20
"""
cursor.execute(number_guests)
result = cursor.fetchone()
print("Number of guests booked between 18:00 and 20:00:", result[0])
print()

# The full name and BookingID of all guests waiting to be seated with the receptionist in sorted order with respect to their BookingSlot
all_guests = """
SELECT CONCAT(g.FirstName, ' ', g.LastName) AS FullName, b.BookingID
FROM Bookings b
JOIN Guest g ON b.GuestID = g.GuestID
WHERE b.TableNumber IS NULL
ORDER BY b.BookingSlot ASC
"""
cursor.execute(all_guests)
results = cursor.fetchall()
print("Guests waiting to be seated:")
for result in results:
    print("Name:", result[0])
    print("BookingID:", result[1])
    print()

    
# Task 4

sales_report = """
CREATE PROCEDURE BasicSalesReport()
BEGIN
    SELECT
        SUM(BillAmount) AS TotalSales,
        AVG(BillAmount) AS AverageSale,
        MIN(BillAmount) AS MinimumBillPaid,
        MAX(BillAmount) AS MaximumBillPaid
    FROM Bill;
END
"""

cursor.execute(sales_report)

#Task 5
# Get a connection from the pool
connection = pool_b.get_connection()

# Create a buffered cursor
cursor = connection.cursor(buffered=True)

# SQL query to retrieve the next three bookings with assigned employees
bookings = """
    SELECT b.BookingSlot, CONCAT(g.FirstName, ' ', g.LastName) AS Guest_name, CONCAT(e.FirstName, ' ', e.LastName, ' [', e.Role, ']') AS Assigned_to
    FROM Bookings b
    JOIN Guests g ON b.GuestID = g.GuestID
    JOIN Employee e ON b.EmployeeID = e.EmployeeID
    ORDER BY b.BookingSlot ASC
    LIMIT 3;
"""

# Execute the query
cursor.execute(bookings)

# Fetch the results
results = cursor.fetchall()

# Print the results
print("[BookingSlot]\t\t[Guest_name]\t\t\t[Assigned to: Employee Name [Employee Role]]")
for row in results:
    print(row[0], "\t", row[1], "\t", row[2])

# Close the cursor and connection
cursor.close()
connection.close()

# Return the connection to the pool
pool_b.put_connection(connection)


# In[ ]:




