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
        gpu=1,
    )
    @step
    def whisper(self):
        """Run the transcript through Whisper"""
        from shutil import copy
        from subprocess import run

        copy(f'/m/{self.guid}', '/')
        self.cmd = [
            'whisper',
            '--model',
            self.model,
            '-o',
            '/m',
            f'/{self.guid}',
        ]
        print('Running:', self.cmd)
        run(self.cmd, check=True)

        self.next(self.end)

    @kubernetes(
        image='ghcr.io/wgbh-mla/mario:pr-2',
        persistent_volume_claims={
            'media-pvc': '/m',
        },
    )
    @step
    def end(self):
        """Report results and cleanup"""
        from glob import glob
        from os import remove
        from subprocess import run

        from boto3 import client

        run(['ls', '-al', '/m'])
        # Hack to remove the extension until we fix the guid list
        filename = self.guid.split('.')[0]

        # Upload transcript to aws
        client = client('s3')
        client.upload_file(
            f'/m/{filename}.json',
            'clams-transcripts',
            f'{filename}/{self.model}/{filename}.json',
        )
        print(f'Successfully processed {self.guid}')

        # delete media file and transcripts
        cleaned = 0
        for f in glob(f'/m/{filename}*'):
            remove(f)
            cleaned += 1
        print(f'Cleaned up {cleaned} files')


if __name__ == '__main__':
    Whisper()
