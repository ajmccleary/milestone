import json
import os
import pandas as pd
from pandas import json_normalize
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from google.cloud.sql.connector import Connector

# Authors: Andrew McCleary and Brady Galligan

# Set this to "GCP" or "AWS" (or use an env var: DB_PLATFORM=GCP/AWS)
PLATFORM = os.getenv("DB_PLATFORM", "GCP").upper()

def getconn():
    # ----- GCP ONLY -----
    # install: pip install PyMySQL cloud-sql-python-connector
    
    # NOTE: use a raw string for Windows paths to avoid backslash escapes
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"dev-smoke-471717-u5-543202eefe0b.json"

    connector = Connector()
    return connector.connect(
        "dev-smoke-471717-u5:us-central1:my-sql",   # e.g. "csc-ser325:us-central1:db325-instance"
        "pymysql",
        user="root",
        password="password",
        db=None               # or None if you prefer to CREATE first, then USE
    )

def setup_db(cur):
  # Set up db
    cur.execute('CREATE DATABASE IF NOT EXISTS nobel_prizes_db;')
    cur.execute('USE nobel_prizes_db;')

    cur.execute('DROP TABLE IF EXISTS NobelPrize_Laureates;')
    cur.execute('DROP TABLE IF EXISTS NobelPrize;')
    cur.execute('DROP TABLE IF EXISTS Category;')
    cur.execute('DROP TABLE IF EXISTS Laureates;')    

    cur.execute('''
        CREATE TABLE Category (
        CategoryId INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
        Name VARCHAR(50) NOT NULL UNIQUE
        );''')

    cur.execute('''CREATE TABLE Laureates (
        LaureateId INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
        FirstName VARCHAR(25),
        LastName VARCHAR(25)
        );''')

    cur.execute('''CREATE TABLE NobelPrize (
        PrizeId INT AUTO_INCREMENT PRIMARY KEY,
        Year INT,
        CategoryId INT,
        FOREIGN KEY(CategoryId) REFERENCES Category(CategoryId)
        );''')
    
    cur.execute('''CREATE TABLE NobelPrize_Laureates (
        Id INT AUTO_INCREMENT PRIMARY KEY,
        Motivation VARCHAR(500),
        Share INT,
        PrizeId INT,
        LaureateId INT,
        FOREIGN KEY(PrizeId) REFERENCES NobelPrize(PrizeId),
        FOREIGN KEY(LaureateId) REFERENCES Laureates(LaureateId)
        );''')

def insert_data(cur):
    cur.execute('USE nobel_prizes_db')

    req = requests.get("https://api.nobelprize.org/v1/prize.json")
    data = req.json()

    prizes = data['prizes']
    rows = []
    for p in prizes:
        year = int(p.get('year')) if p.get('year') and p.get('year').isdigit() else None
        category = p.get('category')
        laureates = p.get('laureates') or []
        if laureates:
            for l in laureates:
                rows.append({
                    'year': year,
                    'category': category,
                    'laureate_id': l.get('id'),
                    'firstname': l.get('firstname'),
                    'surname': l.get('surname'),
                    'motivation': l.get('motivation'),
                    'share': int(l.get('share')) if l.get('share') and l.get('share').isdigit() else None
                })
        else:
            # some prizes may have no laureates listed — include as prize-only row
            rows.append({'year': year, 'category': category, 'laureate_id': None,
                         'firstname': None, 'surname': None, 'motivation': None, 'share': None})

    # Insert unique categories using executemany
    categories = set(row['category'] for row in rows if row['category'])
    category_data = [(category,) for category in categories]
    cur.executemany('INSERT IGNORE INTO Category (Name) VALUES (%s)', category_data)
    
    # Create mapping of category names to IDs
    category_id_map = {}
    for category in categories:
        cur.execute('SELECT CategoryId FROM Category WHERE Name = %s', (category,))
        result = cur.fetchone()
        if result:
            category_id_map[category] = result[0]
    
    # Batch insert laureates
    unique_laureates = {}
    for row in rows:
        if row['laureate_id'] and row['laureate_id'] not in unique_laureates:
            unique_laureates[row['laureate_id']] = (row['firstname'], row['surname'])
    
    laureate_data = [(firstname, surname) for firstname, surname in unique_laureates.values()]
    cur.executemany('''INSERT IGNORE INTO Laureates (FirstName, LastName) 
                      VALUES (%s, %s)''', laureate_data)
    
    # Create laureate ID mapping
    laureate_id_map = {}
    for laureate_id, (firstname, surname) in unique_laureates.items():
        cur.execute('SELECT LaureateId FROM Laureates WHERE FirstName = %s AND LastName = %s', 
                   (firstname, surname))
        result = cur.fetchone()
        if result:
            laureate_id_map[laureate_id] = result[0]
    
    # Batch insert Nobel Prizes
    unique_prizes = set()
    for row in rows:
        if row['year'] and row['category']:
            unique_prizes.add((row['year'], row['category']))
    
    prize_data = [(year, category_id_map[category]) for year, category in unique_prizes]
    cur.executemany('''INSERT IGNORE INTO NobelPrize (Year, CategoryId) 
                      VALUES (%s, %s)''', prize_data)
    
    # Create prize ID mapping
    prize_map = {}
    for year, category in unique_prizes:
        category_id = category_id_map[category]
        cur.execute('SELECT PrizeId FROM NobelPrize WHERE Year = %s AND CategoryId = %s', 
                   (year, category_id))
        result = cur.fetchone()
        if result:
            prize_map[(year, category)] = result[0]
    
    # Batch insert into NobelPrize_Laureates junction table
    junction_data = []
    for row in rows:
        if row['laureate_id'] and row['year'] and row['category']:
            key = (row['year'], row['category'])
            prize_id = prize_map.get(key)
            laureate_id = laureate_id_map.get(row['laureate_id'])
            
            if prize_id and laureate_id:
                junction_data.append((row['motivation'], row['share'], prize_id, laureate_id))
    
    # THIS IS THE KEY PERFORMANCE LINE - batch insert all junction records at once
    cur.executemany('''INSERT IGNORE INTO NobelPrize_Laureates 
                      (Motivation, Share, PrizeId, LaureateId) 
                      VALUES (%s, %s, %s, %s)''', junction_data)

def select_all_data(cur):
    cur.execute('USE nobel_prizes_db')
    cur.execute('''
        SELECT np.Year, c.Name AS Category, l.FirstName, l.LastName, npl.Motivation, npl.Share
        FROM NobelPrize_Laureates npl
        JOIN NobelPrize np ON npl.PrizeId = np.PrizeId
        JOIN Category c ON np.CategoryId = c.CategoryId
        JOIN Laureates l ON npl.LaureateId = l.LaureateId
        ORDER BY np.Year, c.Name;
    ''')
    results = cur.fetchall()
    for row in results:
        print(row)

# ----- MAIN PROGRAM -----
with getconn() as conn:
    if conn.open:
        print("Connected to GCP Cloud SQL")
    with conn.cursor() as cur:
        setup_db(cur)
        insert_data(cur)
        select_all_data(cur)
        conn.commit()