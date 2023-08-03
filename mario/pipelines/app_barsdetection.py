from metaflow import FlowSpec, Parameter, kubernetes, secrets, step, trigger


@trigger(event='app-barsdetection')
class AppBarsdetection(FlowSpec):
    """Run a transcript through app-barsdetection"""

    guid = Parameter('guid', help='GUID of the transcript to process')
    mmif = Parameter('mmif', help='GUID of the transcript to process', default=None)

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

        assert self.asset_id, f'No asset found for {self.guid}'
        print(f'Found asset {self.asset_id}')
        filename = join('/m', self.guid + '.mp4')

        # get mmif
        self.input_mmif = self.mmif
        if not self.mmif:
            print('No mmif provided, downloading from clams')
            self.input_mmif = post(
                'http://fastclam/source',
                json={'files': ['video:' + filename]},
            ).json()
        # Download the media file
        # run(f'ci download {self.asset_id} -o {join("/m", self.guid)}')
        ci = SonyCi(**SonyCi.from_env())
        self.asset = ci.get(f'assets/{self.asset_id}')

        url = self.asset['proxyUrl']
        print('Downloading file')
        urlretrieve(url, filename)

        print('Downloaded file')
        run(['ls', '-al', '/m'])

        self.next(self.barsdetection)

    @kubernetes(image='ghcr.io/wgbh-mla/mario:main')
    @step
    def barsdetection(self):
        """Run the mmif through app-barsdetection"""
        from requests import post

        self.response = post('http://app-barsdetection/', json=self.input_mmif)
        if self.response.status_code != 200:
            print(self.response.content)
            from mario.utils import CLAMSAppError

            raise CLAMSAppError(
                f'app-barsdetection failed: {self.response.status_code} - {self.response.content}'
            )
        self.output_mmif = self.response.json()
        print('Response from app-barsdetection:')
        print(self.output_mmif)

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

        filename = join(self.guid, '.mmif')
        s3_path = f'{self.guid}/app-barsdetection/{self.guid}.mmif'

        with open(join('/m', filename), 'w') as f:
            f.write(dumps(self.output_mmif))

        run(['ls', '-al', '/m'])

        # Upload transcript to aws
        print(f'Uploading {filename} to {s3_path}')
        client = client('s3')
        client.upload_file(
            join('/m', filename),
            'clams-transcripts',
            s3_path,
        )
        print(f'Successfully processed {self.guid}')

        # delete media file and transcripts
        cleaned = 0
        for f in glob(f'/m/{filename}*'):
            remove(f)
            cleaned += 1
        print(f'Cleaned up {cleaned} files')


if __name__ == '__main__':
    AppBarsdetection()
