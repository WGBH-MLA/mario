from requests import post
from requests_oauth2client.tokens import BearerToken

from mario.config import API_AUDIENCE, AUTH0_DOMAIN, CLIENT_ID, CLIENT_SECRET
from mario.log import log


def get_token(
    client_id: str = CLIENT_ID,
    client_secret: str = CLIENT_SECRET,
    token_url: str = AUTH0_DOMAIN,
    grant_type: str = 'client_credentials',
    audience: str = API_AUDIENCE,
) -> BearerToken:
    """Login and return a BearerToken."""
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
        raise Exception(
            f'Token did not return 200. Returned: {response.status_code}: {response.text}'
        )
    return BearerToken(**response.json())
