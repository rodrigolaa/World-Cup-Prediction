# %%
# PYTHON PACKAGES
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
#from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
import google.auth.transport.requests
#from google.auth.transport.requests import AuthorizedSession
import gspread
from datetime import datetime


# %%
# INITIAL CONFIGS
now = datetime.now()
today = now.strftime("%d/%m/%Y")  # GET THE CURRENT DAY
# LOCATION OF .JSON FILE WITH CREDENTIALS OF GOOGLE API
path_sheets = ".\sheets-credentials.json"
# CREATE A .TXT FILE WITH THE CLIENT ID AND SECRETE OF SPOTIFY API
path_spotify = ".\spotify_confidential.txt.txt"
cid = open(path_spotify, "r").read().split('\n')[0]  # SPOTIFY CREDENTIALS
secret = open(path_spotify, "r").read().split('\n')[1]  # SPOTIFY CREDENTIALS
key = '18MFV_mk8PAem4aaO0Psl6SMTmH6TCsdLNZGJvLsGNVQ'  # GOOGLE SHEETS KEY


# %%
def pandas_df_to_googlesheets_(key, Data, path=path_sheets):
    Dataset = Data.copy().astype(str)  # COPY DATAFRAME
    Valores = Dataset.values  # GET ALL VALUES
    Columnas = Dataset.columns.values  # GET ALL COLUMNS NAMES

    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']  # Apis autorizadas
    credentials = Credentials.from_service_account_file(
        path, scopes=scopes)  # Se lee lacredencial

    request = google.auth.transport.requests.Request()

    credentials.refresh(request)

    gc = gspread.authorize(credentials)
    sht = gc.open_by_key(str(key))  # GET THE SHEETS WITH THE KEY

    worksheet = sht.worksheet('RawData')  # EXTRACT THE SPECIFIED SHEETS

    response = worksheet.get_values('A1:A')  # GET ALL VALUES IN FIRST COLUMN

    last_row = str(len(response)+1)  # GET THE LAST EMPTY ROW AVAILABLE

    # UPDATE SHEET WITH ONLY THE VALUES STARTING IN THE FIRST EMPTY ROW
    worksheet.update("A" + last_row, Valores.tolist())


# %%
# Authentication - without user
client_credentials_manager = SpotifyClientCredentials(
    client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# %%
# GET TOP 50 TRACKS PLAYLIST
playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=1333723a6eff4b7f"
playlist_URI = playlist_link.split("/")[-1].split("?")[0]
track_uris = [x["track"]["uri"]
              for x in sp.playlist_tracks(playlist_URI)["items"]]

# %%
# CREATE EMPTY LIST
lst_data = []

# %%
# LOOP THROUGH TRACKS AND CREATING DATASET WITH PROCESSED DATA

def lambda_handler(event, context):
    for track in sp.playlist_tracks(playlist_URI)["items"]:
            # URI
        track_uri = track["track"]["uri"]

    # Track name
        track_name = track["track"]["name"]

    # Main Artist
        artist_uri = track["track"]["artists"][0]["uri"]
        artist_info = sp.artist(artist_uri)

    #Name, popularity, genre
        artist_name = track["track"]["artists"][0]["name"]
        artist_pop = artist_info["popularity"]
        artist_genres = artist_info["genres"]
        artist_url = artist_info["external_urls"]['spotify']
        artist_followers = artist_info["followers"]['total']
        artist_image = artist_info["images"][0]['url']

    # Album
        album = track["track"]["album"]["name"]

    # Popularity of the track
        track_pop = track["track"]["popularity"]

    # COMPILE ALL VARIABLES IN A LIST
        data = [
            today,
            track_uri,
            track_name,
            artist_uri,
            artist_info,
            artist_name,
            artist_pop,
            artist_genres,
            album,
            track_pop,
            artist_url,
            artist_followers,
            artist_image
        ]
    # CREATE A PANDAS DATAFRAME OF EACH LIST
        df1 = pd.DataFrame([data], columns=['date', 'track_uri', 'track_name', 'artist_uri',
                       'artist_info', 'artist_name', 'artist_pop', 'artist_genres', 'album', 'track_pop','artist_url','artist_followers','artist_image'])
    # COMPILE EACH DATAFRAME IN THE EMPTY LIST PREVIOUSLY CREATED
        lst_data.append(df1)

    # JOIN ALL DATAFRAMES IN THE LIST IN A SINGLE FINAL ONE
    df = pd.concat(lst_data, ignore_index=True)
    pandas_df_to_googlesheets_(key, df)

# %%
