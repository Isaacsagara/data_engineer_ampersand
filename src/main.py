#!/usr/bin/env python

import csv
import json
import psycopg2
import pandas as pd



# connect to the database
connection = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="postgres",
    database="data_engineer"
)

# Create a cursor
cursor = connection.cursor()

with open('db_schema.sql', 'r') as db:
    cursor.execute(db.read())
    connection.commit()


# Read the places CSV file into a dataframe
df_places = pd.read_csv('data/places.csv')

# Assign a default index to the dataframe
df_places.index += 1

# Load data into the places table
df = pd.read_csv('data/places.csv')
for index, row in df.iterrows():
    cursor.execute("INSERT INTO places (city, county, country) VALUES (%s, %s, %s)", row)
connection.commit()

# Read the CSV file into a dataframe
df_people = pd.read_csv('data/people.csv')

# Assign a default index to the dataframe
df_people.index += 1

# Insert city_birth_id column on people dataframe to hold city_id with default value of 1
df_people['city_birth_id']= 1

# Assigning city_id on people dataframe where city in place table matches the city of birth in people dataframe
for j in range(1,len(df_places)):
    for i in range(1,len(df_people)+1):
        if df_places['city'][j] == df_people['place_of_birth'][i]:
            df_people['city_birth_id'][i] = df_places.index[j-1]
            
# Drop place_of_birth name to remain with city_of_birth id
df_people = df_people.drop(['place_of_birth'], axis=1)

# Load data into people table
for index, row in df_people.iterrows():
    cursor.execute("INSERT INTO people (f_name, l_name, date_of_birth, city_of_birth_id)\
                     VALUES (%s, %s, %s, %s)", row)
    
# Commit the changes
connection.commit()

# Execute the SELECT statement to retrieve  the table data of number of people born in respective countries.
cursor.execute("SELECT country,\
                COUNT(places.id)\
                FROM places, people\
                WHERE people.city_of_birth_id = places.id\
                GROUP BY country")

# Write the data to a JSON file
with open('data/summary_output.json', 'w') as f:
    rows = cursor.fetchall()
    data = dict((x, y) for x, y in rows)
    json.dump(data, f, separators=(',', ':'))

# Close the cursor and connection
cursor.close()
connection.close()