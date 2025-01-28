from pymongo import MongoClient
import pandas as pd

# Connessione al server MongoDB
client = MongoClient('localhost', 27017)

# Seleziona il database
db = client['IUM']

# Funzione per inserire i dati in una collezione
def insert_data(collection_name, dataframe):
    collection = db[collection_name]
    data_dict = dataframe.to_dict("records")
    collection.insert_many(data_dict)
    print(f"Inserted {len(data_dict)} records into collection {collection_name}")

# Carica i dataframe
actors_df = pd.read_csv('Output/actors_cleaned.csv')
countries_df = pd.read_csv('Output/countries_cleaned.csv')
crews_df = pd.read_csv('Output/crews_cleaned.csv')
genres_df = pd.read_csv('Output/genres_cleaned.csv')
languages_df = pd.read_csv('Output/languages_cleaned.csv')
movies_df = pd.read_csv('Output/movies_cleaned.csv')
posters_df = pd.read_csv('Output/posters_cleaned.csv')
releases_df = pd.read_csv('Output/releases_cleaned.csv')
studios_df = pd.read_csv('Output/studios_cleaned.csv')
themes_df = pd.read_csv('Output/themes_cleaned.csv')
rotten_tomatoes_df = pd.read_csv('Output/rotten_tomatoes_cleaned.csv')
oscar_df = pd.read_csv('Output/oscar_cleaned.csv')

# Inserisci i dati nelle collezioni
insert_data('actors_cleaned', actors_df)
insert_data('countries_cleaned', countries_df)
insert_data('crews_cleaned', crews_df)
insert_data('genres_cleaned', genres_df)
insert_data('languages_cleaned', languages_df)
insert_data('movies_cleaned', movies_df)
insert_data('posters_cleaned', posters_df)
insert_data('releases_cleaned', releases_df)
insert_data('studios_cleaned', studios_df)
insert_data('themes_cleaned', themes_df)
insert_data('rotten_tomatoes_cleaned', rotten_tomatoes_df)
insert_data('oscar_cleaned', oscar_df)

actors_collection = db['actors_cleaned']
actors = actors_collection.find().limit(10)  # Limita il risultato a 10 documenti

# Stampa i documenti trovati
for actor in actors:
    print(actor)
# Chiudi la connessione
client.close()