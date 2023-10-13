class PipelineUtils:
    """Utility functions for CLAMS Pipeline Runner"""

    def get_asset_id(self) -> None:
        from chowda.db import engine
        from chowda.models import Batch, MediaFile, MetaflowRun
        from metaflow import current
        from sqlmodel import Session, select

        """Get the asset.id + filename for a media file"""
        from os.path import join

        with Session(engine) as db:
            media_file = db.exec(
                select(MediaFile).where(MediaFile.guid == self.guid)
            ).one()

            # Add the Metaflow Run to the database
            batch = db.get(Batch, self.batch_id)
            pathspec = current.flow_name + '/' + current.run_id
            print(f'Adding {current.run_id} to {batch.name} with {pathspec}')
            new_metaflow_run = MetaflowRun(
                id=current.run_id, batch=batch, media_file=media_file, pathspec=pathspec
            )
            db.add(new_metaflow_run)
            db.commit()

            # Filter for media assets
            assets = [
                asset
                for asset in media_file.assets
                if asset.name.endswith(('.mp4', '.mp3'))
            ]
            assert assets, f'No media assets found for {self.guid}'
            asset = assets[0]
            self.asset_id = asset.id
            self.asset_name = asset.name
            self.type = asset.type.value.lower()
            self.filename = join('/m', self.asset_name)

    def create_new_mmif(self) -> dict:
        from requests import post

        self.input_mmif = post(
            'http://fastclam/source',
            json={'files': [f'{self.type}:' + self.filename]},
        ).json()

    def download_media_file(self) -> None:
        """Download the media file"""
        from urllib.request import urlretrieve

        from sonyci import SonyCi

        ci = SonyCi(**SonyCi.from_env())
        self.asset = ci.get(f'assets/{self.asset_id}')

        url = self.asset['proxyUrl']
        print('Downloading file')
        urlretrieve(url, self.filename)

    def app(self, app: str, mmif: dict) -> dict:
        """Run the mmif through a CLAMS app"""
        from requests import post

        response = post(app, json=mmif)
        if response.status_code != 200:
            from mario.utils import CLAMSAppError

            raise CLAMSAppError(
                f'{app} failed: {response.status_code} - {response.content}'
            )
        return response.json()

    def update_database(self, s3_path: str) -> None:
        """Update the database with the output mmif"""
        from chowda.db import engine
        from chowda.models import MMIF, Batch, MediaFile, MetaflowRun
        from metaflow import current
        from sqlmodel import Session, select

        with Session(engine) as db:
            media_file = db.exec(
                select(MediaFile).where(MediaFile.guid == self.guid)
            ).one()
            batch = db.get(Batch, self.batch_id)
            metaflow_run = db.get(MetaflowRun, current.run_id)
            mmif = MMIF(
                media_file=media_file,
                metaflow_run=metaflow_run,
                batch_output=batch,
                mmif_location=s3_path,
            )
            db.add(mmif)
            db.commit()

    def download_mmif(self, s3_path: str) -> dict:
        """Download an mmif from S3"""
        from json import loads

        from boto3 import client

        bucket = self.bucket if self.bucket != 'null' else 'clams-mmif'
        client = client('s3')
        response = client.get_object(Bucket=bucket, Key=s3_path)
        body = response['Body'].read().decode('utf-8')
        return loads(body)

    def upload_mmif(self) -> None:
        """Upload the output mmif to S3"""
        from json import dumps
        from os.path import join

        from boto3 import client

        mmif_filename = join('/m', self.guid + '.mmif')
        s3_path = f'{self.guid}/{self.batch_id}/{self.guid}.mmif'

        with open(mmif_filename, 'w') as f:
            f.write(dumps(self.output_mmif))

        # Upload transcript to aws
        bucket = self.bucket if self.bucket != 'null' else 'clams-mmif'
        print(f'Uploading {mmif_filename} to {bucket} {s3_path}')
        client = client('s3')
        client.upload_file(
            mmif_filename,
            bucket,
            s3_path,
        )

    def cleanup(self) -> None:
        """delete media file and transcripts"""
        from glob import glob
        from os import remove

        cleaned = 0
        for f in glob(f'/m/{self.guid}*'):
            remove(f)
            cleaned += 1
        print(f'Cleaned up {cleaned} files')
