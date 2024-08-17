import re
import json
import pandas as pd
from pymongo import MongoClient
import typer
from tqdm import tqdm
import numpy as np

# Create a Typer app instance for command-line interface
app = typer.Typer()


@app.command()
def insert_data(tsv_path: str):

    """Insert data from TSV file into MongoDB"""
    # Read data from a TSV file
    df = pd.read_csv(tsv_path, sep="\t")

    # Select relevant columns
    new_df = df[['text', 'id', 'ts1', 'place_id', 'like_count', 'author_handle']]
    data_dict = new_df.to_dict(orient='records')

    # Establish connection to MongoDB
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=100000)
    db = client['tweets']
    collection = db['tweets_by_britney']

    # Create an index on the 'id' field to ensure uniqueness
    collection.create_index("id", unique=True)
    
    # Insert data into mongoDB collection
    for i in tqdm(data_dict):
        collection.insert_one(i)
    typer.echo("Data inserted successfully.")

@app.command()
def fetch_data(term : str):
    #Fetch data based on queries and display results in JSON format
    results = {}
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=50000)
    db = client['tweets']
    collection = db['tweets_by_britney']
    

    # Fetch :data Query 1: Get daily count of documents matching the term
    results['daily_count'] = list(collection.aggregate([
        {'$match': {'text': re.compile(term)}},
        {'$group': {'_id': {'day': {'$dayOfYear': {'$toDate': '$ts1'}}}, 'count': {'$count': {}}}}
    ]))

    # Query 2: Get unique author handles matching the term
    results['unique_authors'] = list(collection.aggregate([
        {'$match': {'text': re.compile(term)}},
        {'$group': {'_id': {}, 'uniqueAuthors': {'$addToSet': '$author_handle'}}},
        {'$project': {'_id': 0, 'uniqueAuthors': {'$size': '$uniqueAuthors'}}}
    ]))


    # Query 3: Get average likes for documents matching the term
    results['avg_likes'] = list(collection.aggregate([
        {'$match': {'text': re.compile(term)}},
        {'$group': {'_id': {}, 'avgLikes': {'$avg': '$like_count'}}}
    ]))

    # Query 4: Get unique place IDs matching the term (excluding NaN)
    results['unique_place_ids'] = list(collection.aggregate([
        {'$match': {'text': re.compile(term), 'place_id': {'$ne': np.nan}}},
        {'$group': {'_id': {}, 'uniquePlaceIDs': {'$addToSet': '$place_id'}}},
        {'$project': {'_id': 0, 'uniquePlaceIDs': 1}}
    ]))


    # Query 5: Get hourly count of documents matching the term
    results['hourly_count'] = list(collection.aggregate([
        {'$match': {'text': re.compile(term)}},
        {'$addFields': {'time': {'$substr': [{'$arrayElemAt': [{'$split': ['$ts1', ' ']}, 1]}, 0, 8]}}},
        {'$group': {'_id': {'time': '$time'}, 'count': {'$sum': 1}}}
    ]))


    # Query 6: Get the author with the highest tweet count
    results['top_author'] = list(collection.aggregate([
        {'$match': {'text': re.compile(term)}},
        {'$group': {'_id': '$author_handle', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 1},
        {'$project': {'mostTweeter': '$_id'}}
    ]))

    # Process results
    data_day = []
    for i in results['daily_count']:
        data_day.append(i['_id'])

    data_time = {}
    for i in results['hourly_count']:
        data_time[i['_id'].get('time',0)] = i.get('count',0)

    data = {'daily_count': data_day,
            "unique_authors": results['unique_authors'][0]['uniqueAuthors'],
            "avrage_likes": round(results['avg_likes'][0]['avgLikes'],2),
            "unique_palce_id" : results['unique_place_ids'][0]['uniquePlaceIDs'],
            "timly_count": data_time,
            "most_tweeted" : results['top_author'][0]['mostTweeter']
            }
    
     # Write results to a JSON file
    with open("result.json","w") as js:
        json.dump(data,js)
    

    #Print results
    typer.echo(json.dumps(data,indent=2))


#Entry point for the typer app
if __name__ == "__main__":
    app()