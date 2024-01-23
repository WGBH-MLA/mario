from metaflow import FlowSpec, step


class TokenFlow(FlowSpec):
    @step
    def start(self):
        """Login and get a BearerToken."""
        from requests import post
        from requests_oauth2client.tokens import BearerToken

        from mario.config import API_AUDIENCE, AUTH0_DOMAIN, CLIENT_ID, CLIENT_SECRET
        from mario.log import log

        response = post(
            AUTH0_DOMAIN,
            data={
                'grant_type': 'client_credentials',
                'client_id': CLIENT_ID,
                'client_secret': CLIENT_SECRET,
                'audience': API_AUDIENCE,
            },
        )
        log.debug(f'token response status: {response.status_code}')
        if response.status_code != 200:
            log.error(f'Token returned {response.status_code}: {response.text}')
            raise Exception(
                f'Token did not return 200. Returned: {response.status_code}: {response.text}'
            )
        bearer_token = BearerToken(**response.json())
        self.access_token = bearer_token.access_token
        self.next(self.end)

    @step
    def end(self):
        """Upload the token as a kubernetes secret."""
        from json import dumps
        from subprocess import run

        print('Updating kubernetes secret...')
        access_token = f'Bearer {self.access_token}'
        patch = {'stringData': {'token': access_token}}
        cmd = 'kubectl -n argo-events patch secret argo-events-chowda-api-token -p'.split()
        cmd.append(dumps(patch))
        print('running: ', cmd)
        results = run(cmd, capture_output=True, text=True)
        print(results.stderr)
        print(results.stdout)
        print('Done! Return code: ', results.returncode)
        if results.returncode != 0:
            raise Exception('Error updating kubernetes secret')


if __name__ == '__main__':
    TokenFlow()
