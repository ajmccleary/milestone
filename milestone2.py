import json
import pandas as pd
from pandas import json_normalize
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

req = requests.get("https://api.nobelprize.org/v1/prize.json")
data = req.json()

#Janky simmed SQL thing (converting JSON to actual accessible variables)
prizes = data['prizes']
#Stolen bastardized StackOverflow code to make the JSON file actually accessible in Python (I miss R)
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

df = pd.DataFrame(rows)
df['fullname'] = df[['firstname','surname']].fillna('').agg(' '.join, axis=1).str.strip()
df.head()


# compute number of laureates per prize (year+category defines a single prize)
prize_counts = df.groupby(['year','category']).agg(laureates_per_prize=('laureate_id','nunique')).reset_index()
yearly = prize_counts.groupby('year').laureates_per_prize.mean().reset_index()

plt.figure(figsize=(12,5))
sns.lineplot(data=yearly, x='year', y='laureates_per_prize', marker='o')
plt.title('Average laureates per prize by year')
plt.xlabel('Year')
plt.ylabel('Avg laureates per prize')
plt.xlim(min(yearly.year), max(yearly.year))
plt.show()

# count unique prizes by category: unique (year,category) pairs
prizes_per_cat = df.groupby('category').apply(lambda g: g[['year','category']].drop_duplicates().shape[0]).reset_index(name='prize_count')
prizes_per_cat = prizes_per_cat.sort_values('prize_count', ascending=False)

plt.figure(figsize=(10,6))
sns.barplot(data=prizes_per_cat, y='category', x='prize_count')
plt.title('Total prizes awarded per category')
plt.xlabel('Number of prizes')
plt.ylabel('Category')
plt.show()

prizes_per_year = df.groupby(['year','category']).size().reset_index(name='count').groupby('year').size().reset_index(name='prizes_count')
# simpler: unique (year,category)
prizes_per_year = df[['year','category']].drop_duplicates().groupby('year').size().reset_index(name='prizes_count')

plt.figure(figsize=(12,5))
sns.barplot(data=prizes_per_year, x='year', y='prizes_count')
plt.xticks(rotation=90)
plt.title('Prizes awarded per year')
plt.xlabel('Year')
plt.ylabel('Number of prizes')
plt.show()

categories_per_year = df[['year','category']].drop_duplicates().groupby('year').size().reset_index(name='num_categories')

plt.figure(figsize=(12,4))
sns.lineplot(data=categories_per_year, x='year', y='num_categories', marker='o')
plt.title('Number of categories with awards each year')
plt.xlabel('Year')
plt.ylabel('Number of categories')
plt.show()

# Count distinct (year,category) per laureate (some laureates have multiple prizes)
recurring = df.dropna(subset=['laureate_id']).groupby(['fullname']).apply(lambda g: g[['year','category']].drop_duplicates().shape[0]).reset_index(name='prize_count')
recurring = recurring[recurring['prize_count'] > 1].sort_values('prize_count', ascending=False)
recurring.head(20)
top_recurring = recurring.head(10)
plt.figure(figsize=(10,5))
sns.barplot(data=top_recurring, x='prize_count', y='fullname')
plt.title('Top recurring laureates (more than one prize)')
plt.xlabel('Number of distinct prizes')
plt.ylabel('Laureate')
plt.show()
