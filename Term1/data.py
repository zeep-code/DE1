import pandas as pd
import mysql.connector
from mysql.connector import Error

def create_connection():
    try:
        conn = mysql.connector.connect(user='root', 
                                       password='r00ot1234',
                                       host='localhost',
                                       database='addidas_sales',
                                       use_pure=True)
        if conn.is_connected():
            print("Connection to MySQL is successful!")
            return conn
    except Error as e:
        print("Error while connecting to MySQL", e)
        return None

def insert_data(cursor, query, data):
    try:
        # Convert list of lists to list of tuples
        data_to_insert = [tuple(row) for row in data]
        print(f"Executing query: {query}")  # Debugging line
        print(f"With data: {data_to_insert[:5]}")  # Print first 5 data entries for debugging
        cursor.executemany(query, data_to_insert)
        print(f"Data inserted successfully: {cursor.rowcount} rows added.")
    except Error as e:
        print(f"Error: {e}")


def load_data_to_db(file_path):
    # Read the CSV file
    data = pd.read_csv(file_path)
    print(f"Number of rows loaded from CSV: {len(data)}")  # Debugging line

 # Rename DataFrame columns to match the database column names
    data.rename(columns={
        'Retailer ID': 'Retailer_ID',
        'Retailer': 'Retailer_Name',
        'Product': 'Product_Name',
        'Price per Unit': 'Price_per_Unit',
        'Invoice Date': 'Invoice_Date',  # Renaming this to match your code's expectation
        'Units Sold': 'Units_Sold',
        'Total Sales': 'Total_Sales',
        'Operating Profit': 'Operating_Profit',
        'Operating Margin': 'Operating_Margin',
        'Sales Method': 'Sales_Method'
    }, inplace=True)

    # Transform monetary values to numerical data types
    data['Total_Sales'] = data['Total_Sales'].replace(r'[\$,]', '', regex=True).astype(float)
    data['Operating_Profit'] = data['Operating_Profit'].replace(r'[\$,]', '', regex=True).astype(float)
    data['Price_per_Unit'] = data['Price_per_Unit'].replace(r'[\$,]', '', regex=True).astype(float)

    # Remove commas from 'Units_Sold' and convert to integer
    data['Units_Sold'] = data['Units_Sold'].str.replace(',', '').astype(int)


    # Transform percentage values to numerical data types
    data['Operating_Margin'] = data['Operating_Margin'].replace(r'[%]', '', regex=True).astype(float) / 100

    # Transform the Invoice Date to a datetime
    data['Invoice_Date'] = pd.to_datetime(data['Invoice_Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

    # Print the DataFrame after transformation to check for data integrity
    print(data.head())  # Debugging line

    # Ensure all categories for certain columns are included
    categorical_columns = ['Sales_Method', 'Region', 'State', 'Product_Name']
    data = data.drop_duplicates(subset=categorical_columns)
    print(f"Data after dropping duplicates: {len(data)}")  # Debugging line

    # ... (include the revised sampling logic here, make sure to use the new column names)
# Sample 500 rows from the data
    sampled_data = pd.concat([df.sample(n=1, random_state=1) for _, df in data.groupby(categorical_columns)])
    if len(sampled_data) < 500:
        additional_samples_needed = 500 - len(sampled_data)
        remaining_data = data.loc[~data.index.isin(sampled_data.index)]
        additional_samples_df = remaining_data.sample(n=additional_samples_needed, random_state=1)
        sampled_data = pd.concat([sampled_data, additional_samples_df])
    print(f"Number of rows after sampling: {len(sampled_data)}")  # Debugging line

    # Prepare data for the Retailer table
    unique_retailers = data[['Retailer_ID', 'Retailer_Name']].drop_duplicates(subset=['Retailer_ID', 'Retailer_Name'])                
    retailer_data = unique_retailers.values.tolist()

    # Connect to the database
    connection = create_connection()
    if connection is not None:
        cursor = connection.cursor()

    # Fetch existing retailer data from the database to avoid duplicates
        cursor.execute("SELECT Retailer_ID, Retailer_Name FROM retailer;")
        existing_retailers = cursor.fetchall()
        existing_retailer_set = { (retailer_id, retailer_name) for retailer_id, retailer_name in existing_retailers }

        # Prepare unique retailer data to insert, excluding those that already exist
        unique_retailer_data_to_insert = []
        for retailer_row in retailer_data:
            if (retailer_row[0], retailer_row[1]) not in existing_retailer_set:
                unique_retailer_data_to_insert.append(retailer_row)

      
        # Define the SQL insert queries using the new column names
        # Insert data into retailer table if it doesn't already exist
        insert_retailer_query = """    
            INSERT INTO retailer (Retailer_ID, Retailer_Name) 
            SELECT * FROM (SELECT %s, %s) AS tmp
            WHERE NOT EXISTS (
                SELECT Retailer_ID FROM retailer WHERE Retailer_ID = %s AND Retailer_Name = %s
            ) LIMIT 1;
        """
        for retailer_row in retailer_data:
            cursor.execute(insert_retailer_query, retailer_row * 2) # retailer_row is repeated to fill the placeholders in the WHERE NOT EXISTS subquery


        insert_product_query = """
            INSERT INTO product (Product_Name, Price_per_Unit) 
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE Price_per_Unit = VALUES(Price_per_Unit);
        """
        insert_location_query = """
            INSERT INTO location (City, State, Region) 
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE City = VALUES(City), State = VALUES(State), Region = VALUES(Region);
        """
        insert_transaction_query = """
            INSERT INTO salestransaction (Invoice_Date, Units_Sold, Total_Sales, Operating_Profit, Operating_Margin, Sales_Method, Retailer_Entry_ID, Product_ID, Location_ID) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        # ... (the rest of the data insertion logic, ensure to use the new column names)
        # Check if the retailer data is correct
        retailer_data = sampled_data[['Retailer_ID', 'Retailer_Name']].dropna().values
        print("Retailer data to insert:", retailer_data[:5])  # Debugging line, print first 5 entries

        # Insert data into retailer, product, and location tables
        insert_data(cursor, insert_retailer_query, retailer_data)
        insert_data(cursor, insert_product_query, sampled_data[['Product_Name', 'Price_per_Unit']].values)
        insert_data(cursor, insert_location_query, sampled_data[['City', 'State', 'Region']].values)

        # Commit the changes
        connection.commit()

        cursor.execute("SELECT Retailer_Entry_ID, Retailer_ID, Retailer_Name FROM retailer;")
        retailer_mapping = {(retailer_id, retailer_name): entry_id for entry_id, retailer_id, retailer_name in cursor.fetchall()}

        # Retrieve the newly created Product_ID and Location_ID for transactions
        cursor.execute("SELECT Product_ID, Product_Name FROM product;")
        product_mapping = {name: id for id, name in cursor.fetchall()}

        cursor.execute("SELECT Location_ID, City, State, Region FROM location;")
        location_mapping = {(city, state, region): id for id, city, state, region in cursor.fetchall()}

        # Prepare transaction data for insertion
        transaction_data = []
        for _, row in sampled_data.iterrows():
            retailer_entry_id = retailer_mapping.get((row['Retailer_ID'], row['Retailer_Name']))
            product_id = product_mapping.get(row['Product_Name'])
            location_id = location_mapping.get((row['City'], row['State'], row['Region']))
            if product_id is not None and location_id is not None:
                transaction_data.append((
                    row['Invoice_Date'],
                    row['Units_Sold'],
                    row['Total_Sales'],
                    row['Operating_Profit'],
                    row['Operating_Margin'],
                    row['Sales_Method'],
                    retailer_entry_id,
                    product_id,
                    location_id
                ))

        # Insert data into salestransaction table
        insert_data(cursor, insert_transaction_query, transaction_data)

        # Commit the changes and close the connection
        connection.commit()
        cursor.close()
        connection.close()
        print("Data loaded into database successfully!")

file_path = 'E:/CEU/Fall/Data_Engineering_1/Term_Project_1/Addidas_Sales_Dataset/Addidas_US_Sales_Dataset.csv'
load_data_to_db(file_path)
