from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import os
from dotenv import load_dotenv

def db_session():
  
  load_dotenv()

  cloud_config= {
    'secure_connect_bundle': 'secure-connect-big-data-tp.zip'
  }

  CLIENT_ID = os.getenv("ASTRA_CLIENT_ID")
  CLIENT_SECRET = os.getenv("ASTRA_CLIENT_SECRET")

  auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
  cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
  session = cluster.connect()

  row = session.execute("select release_version from system.local").one()
  if row:
    print(row[0])
  else:
    print("An error occurred.")
  
  return session