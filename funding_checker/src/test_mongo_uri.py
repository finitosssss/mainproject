import os
from dotenv import load_dotenv

# Try both loading .env manually and checking current env
load_dotenv()
uri = os.getenv("MONGO_URI")

if uri:
    print(f"MONGO_URI length: {len(uri)}")
    print(f"MONGO_URI starts with: {uri[:20]}...")
    if "," in uri:
        print("Moneo URI contains a comma!")
else:
    print("MONGO_URI is NOT SET in the environment.")
