from metaflow import FlowSpec, Parameter, kubernetes, secrets, step, trigger


@trigger(event='whisper')
class Whisper(FlowSpec):
    """Run a transcript through Whisper"""

    guid = Parameter('guid', help='GUID of the transcript to process')
    model = Parameter('model', help='Whisper model to use', default='base')

    @secrets(sources=['CLAMS-SonyCi-API', 'CLAMS-chowda-secret'])
    @kubernetes(
        image='ghcr.io/wgbh-mla/chowda:main',
        persistent_volume_claims={'media-pvc': '/m'},
    )
    @step
    def start(self):
        """Download the media file"""
        from os.path import join
        from subprocess import run
        from urllib.request import urlretrieve

        from chowda.db import engine
        from chowda.models import SonyCiAsset
        from sonyci import SonyCi
        from sqlmodel import Session, select

        # get SonyCi Asset ID
        with Session(engine) as db:
            self.asset_id = (
                db.exec(select(SonyCiAsset).where(SonyCiAsset.name == self.guid))
                .one()
                .id
            )
        assert self.asset_id, f'No asset found for {self.guid}'
        print(f'Found asset {self.asset_id}')
        # run(f'ci download {self.asset_id} -o {join("/m", self.guid)}')
        ci = SonyCi(**SonyCi.from_env())
        self.asset = ci.get(f'assets/{self.asset_id}')

        url = self.asset['proxyUrl']
        print('Downloading file')
        urlretrieve(url, join('/m', self.guid))

        print('Downloaded file')
        run(['ls', '-al', '/m'])

        self.next(self.whisper)

    @kubernetes(
        image='ghcr.io/wgbh-mla/whisper-bot:v0.2.0',
        persistent_volume_claims={
            'media-pvc': '/m',
            'whisper-models': '/root/.cache/whisper/',
        },
    )
    @step
    def whisper(self):
        """Run the transcript through Whisper"""
        from os.path import join
        from subprocess import run

        run(['ls', '-al', '/m'])
        self.cmd = [
            'whisper',
            '--model',
            self.model,
            '-o',
            '/m',
            f'/m/{self.guid}',
        ]
        print('Running:', self.cmd)
        run(self.cmd, check=True)

        self.next(self.end)

    @step
    def end(self):
        """Report results"""
        print(f'Successfully processed {self.guid}')

        # Upload transcript to aws

        # delete media file and transcripts


if __name__ == '__main__':
    Whisper()
