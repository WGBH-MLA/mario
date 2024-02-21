"""Utility functions for CLAMS Pipeline Runner"""

# Parameter values that should be treated as None. This is necessary
# because falsy values show up differently when called via CLI vs argo
NUNS = (None, 'null', '')


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
                id=current.run_id,
                batch=batch,
                media_file=media_file,
                pathspec=pathspec,
                current_step=current.step_name,
                current_task=current.task_id,
            )
            db.add(new_metaflow_run)
            db.commit()

            assert media_file.assets, f'No media assets found for {self.guid}'
            # Get the first asset. This is ok, because we are now filtering
            # SonyCiAssets on ingest, so they should all be media.
            asset = media_file.assets[0]
            self.asset_id = asset.id
            self.asset_name = asset.name
            self.type = asset.type.value.lower()
            self.filename = join('/m', self.asset_name)

    def get_mmif_from_database(self):
        from chowda.db import engine
        from chowda.models import MediaFile
        from sqlmodel import Session, select

        with Session(engine) as db:
            media_file = db.exec(
                select(MediaFile).where(MediaFile.guid == self.guid)
            ).one()
            # TODO Ensure this gets the most recent mmif
            location = media_file.mmifs[-1].mmif_location
        # get the mmif from the S3 bucket
        if not location:
            raise ValueError(f'No mmif found for {self.guid}')
        return self.download_mmif_from_s3(location)

    def create_new_mmif(self) -> dict:
        from requests import post

        return post(
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

        bucket = self.bucket if self.bucket not in NUNS else 'clams-mmif'
        client = client('s3')
        response = client.get_object(Bucket=bucket, Key=s3_path)
        body = response['Body'].read().decode('utf-8')
        return loads(body)

    def upload_mmif(self, s3_path: str) -> None:
        """Upload the output mmif to S3"""
        from json import dumps
        from os.path import join

        from boto3 import client

        mmif_filename = join(self.guid + '.mmif')

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
        print('Uploaded mmif!')
        return s3_path

    def download_mmif_from_s3(self, s3_path: str):
        from json import loads
        from os import remove
        from os.path import join

        from boto3 import client

        bucket = self.bucket if self.bucket != 'null' else 'clams-mmif'
        mmif_filename = join(self.guid + '.mmif')

        print(f'Downloading {s3_path} to {mmif_filename}')
        s3_client = client('s3')
        s3_client.download_file(
            bucket,
            s3_path,
            mmif_filename,
        )

        with open(mmif_filename) as file:
            file_contents = file.read()

        remove(mmif_filename)

        return loads(file_contents)

    def cleanup(self) -> None:
        """delete media file and transcripts"""
        from glob import glob
        from os import remove

        cleaned = 0
        for f in glob(f'/m/{self.guid}*'):
            remove(f)
            cleaned += 1
        print(f'Cleaned up {cleaned} files')
