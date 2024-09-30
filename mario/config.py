from os import environ
from dotenv import load_dotenv

load_dotenv()

MEDIA_DIR = environ.get('MEDIA_DIR', '/m')

AUTH0_DOMAIN = environ.get('AUTH0_DOMAIN')
API_AUDIENCE = environ.get('API_AUDIENCE', 'https://chowda.wgbh-mla.org/api')

# mario
CLIENT_ID = environ.get('CLIENT_ID')
CLIENT_SECRET = environ.get('CLIENT_SECRET')
