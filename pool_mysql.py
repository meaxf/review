#!/usr/bin/env python
# coding: utf-8

# In[37]:


from mysql.connector.pooling import MySQLConnectionPool
from mysql.connector import Error

# Define database configurations
dbconfig = {"database":"little_lemon_db", "user":"your_username", "password":"your_password"}

# Create a connection pool with two connections
try:
    pool_a = MySQLConnectionPool(pool_name="pool_a",
                                 pool_size=2,
                                 **dbconfig)
    # Get a connection from the pool and create a cursor object
    conn = pool_a.get_connection()
    cursor = conn.cursor()
    print("Connection established successfully!")
except Error as e:
    print(e)
finally:
    # Close the cursor and connection
    cursor.close()
    conn.close()
    
# Define the stored procedure query for PeakHours
create_peakhours_sp = """
    CREATE PROCEDURE PeakHours()
    BEGIN
        SELECT HOUR(BookingSlot) AS hour, COUNT(*) AS bookings
        FROM Bookings
        GROUP BY HOUR(BookingSlot)
        ORDER BY bookings DESC;
    END"""

try:
    # Get a connection from the pool and create a cursor object
    conn = pool_a.get_connection()
    cursor = conn.cursor()
    
    # Run the stored procedure query and call the stored procedure
    cursor.execute(create_peakhours_sp)
    cursor.callproc('PeakHours')
    
    # Fetch the results and print the sorted data
    dataset = cursor.fetchall()
    col_names = [i[0] for i in cursor.description]
    print(col_names)
    for data in dataset:
        print(data)
except Error as e:
    print(e)
finally:
    # Close the cursor and connection
    cursor.close()
    conn.close()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




