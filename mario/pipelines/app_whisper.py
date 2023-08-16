from metaflow import FlowSpec, Parameter, kubernetes, secrets, step, trigger


@trigger(event='app-whisper')
class AppWhisper(FlowSpec):
    """Run a transcript through CLAMS app-whisper"""

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
        from chowda.models import MediaFile
        from requests import post
        from sonyci import SonyCi
        from sqlmodel import Session, select

        # get SonyCi Asset ID
        with Session(engine) as db:
            media_file = db.exec(
                select(MediaFile).where(MediaFile.guid == self.guid)
            ).one()
            self.asset_id = media_file.assets[0].id
            self.filename = media_file.assets[0].name

        assert self.asset_id, f'No asset found for {self.guid}'
        print(f'Found asset {self.asset_id}')
        # run(f'ci download {self.asset_id} -o {join("/m", self.guid)}')
        ci = SonyCi(**SonyCi.from_env())
        self.asset = ci.get(f'assets/{self.asset_id}')

        url = self.asset['proxyUrl']
        filename = join('/m', self.filename)
        print(f'Downloading file to {filename}')
        urlretrieve(url, filename)

        print('Downloaded file!')
        run(['ls', '-al', '/m'])
        self.mmif = post(
            'http://fastclam/source',
            json={'files': ['video:' + filename]},
        ).json()
        self.next(self.whisper)

    @kubernetes()
    @step
    def whisper(self):
        """Run the transcript through Whisper"""

        from requests import post

        print('Sending mmif to app-whisper')
        self.output_mmif = post('http://app-whisper', json=self.mmif).json()

        self.next(self.end)

    @kubernetes(
        image='ghcr.io/wgbh-mla/mario:main',
        persistent_volume_claims={
            'media-pvc': '/m',
        },
    )
    @step
    def end(self):
        """Report results and cleanup"""
        from glob import glob
        from json import dumps
        from os import remove
        from os.path import join
        from subprocess import run

        from boto3 import client

        run(['ls', '-al', '/m'])
        mmif_filename = join('/m', self.guid + '.mmif')
        s3_path = f'{self.guid}/app-whisper/{self.guid}.mmif'

        with open(mmif_filename, 'w') as f:
            f.write(dumps(self.output_mmif))

        # Upload transcript to aws
        print(f'Uploading {mmif_filename} to {s3_path}')
        client = client('s3')
        client.upload_file(
            mmif_filename,
            'clams-mmif',
            s3_path,
        )
        print(f'Successfully processed {self.guid}')

        # delete media file and transcripts
        cleaned = 0
        for f in glob(f'/m/{self.guid}*'):
            remove(f)
            cleaned += 1
        print(f'Cleaned up {cleaned} files')


if __name__ == '__main__':
    AppWhisper()
