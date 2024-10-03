from requests import post
from requests_oauth2client.tokens import BearerToken, BearerTokenSerializer

from mario.config import API_AUDIENCE, AUTH0_DOMAIN, CLIENT_ID, CLIENT_SECRET
from mario.log import log


class AuthError(Exception):
    pass


def get_token(
    client_id: str = CLIENT_ID,
    client_secret: str = CLIENT_SECRET,
    token_url: str = AUTH0_DOMAIN,
    grant_type: str = 'client_credentials',
    audience: str = API_AUDIENCE,
) -> BearerToken:
    """Login and return a BearerToken."""
    log.debug('Logging in...')
    response = post(
        token_url,
        data={
            'grant_type': grant_type,
            'client_id': client_id,
            'client_secret': client_secret,
            'audience': audience,
        },
    )
    log.debug(f'token response status: {response.status_code}')
    if response.status_code != 200:
        log.error(f'Token returned {response.status_code}: {response.text}')
        raise AuthError(
            f'Token did not return 200. Returned: {response.status_code}: {response.text}'
        )
    log.success('Logged in successfully!')
    return BearerToken(**response.json())


def get_token_from_file(filename: str = '.token') -> BearerToken:
    try:
        with open(filename) as f:
            return BearerTokenSerializer().loads(f.read())

    except FileNotFoundError:
        log.debug(f'Token file {filename} not found')
        raise FileNotFoundError(f'Token file {filename} not found')


def save_token_to_file(token: BearerToken, filename: str = '.token') -> None:
    with open(filename, 'w') as f:
        f.write(BearerTokenSerializer().dumps(token))
        log.debug(f'Token successfully saved to {filename}')


def login():
    # Check for an existing token
    try:
        token = get_token_from_file()
    except FileNotFoundError:
        token = get_token()
        save_token_to_file(token)
    # Check if the token is expired
    if token.is_expired():
        token = get_token()
        save_token_to_file(token)
