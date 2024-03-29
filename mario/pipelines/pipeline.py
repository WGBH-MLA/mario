from metaflow import FlowSpec, Parameter, kubernetes, secrets, step, trigger
from utils import NUNS, PipelineUtils


@trigger(event='pipeline')
class Pipeline(FlowSpec, PipelineUtils):
    """Run a MediaFile through a CLAMS pipeline"""

    guid = Parameter('guid', help='GUID of the transcript to process')
    mmif_location = Parameter(
        'mmif_location', help='S3 location of input MMIF to CLAMS app', default=None
    )
    pipeline = Parameter(
        'pipeline', help='List of CLAMS apps to run media through', separator=','
    )
    bucket = Parameter(
        'bucket', help='S3 bucket to store results in', default='clams-mmif'
    )
    batch_id = Parameter(
        'batch_id', help='Batch ID to store results in', default=None, type=int
    )

    @secrets(sources=['CLAMS-SonyCi-API', 'CLAMS-chowda-secret'])
    @kubernetes(
        image='ghcr.io/wgbh-mla/chowda:main',
        persistent_volume_claims={'media-pvc': '/m'},
    )
    @step
    def start(self):
        """Download the media file and initiliaze the mmif"""

        self.get_asset_id()
        assert self.asset_id, f'No asset found for {self.guid}'
        print(f'Found asset {self.asset_id}')

        if self.mmif_location not in NUNS:
            print('Downloading mmif from', self.mmif_location)
            self.input_mmif = self.download_mmif(self.mmif_location)
        else:
            print('No mmif provided. Sourcing new mmif')
            self.input_mmif = self.create_new_mmif()
        assert self.input_mmif, 'Problem getting mmif'
        print('Got mmif')
        print(self.input_mmif)

        self.download_media_file()
        print('Downloaded file')

        self.next(self.run_pipeline)

    @kubernetes(image='ghcr.io/wgbh-mla/mario:main')
    @step
    def run_pipeline(self):
        """Run the mmif through a CLAMS pipeline"""
        mmif = self.input_mmif
        print('starting pipeline')
        print(self.pipeline)
        for app in self.pipeline:
            print(f'Running {app}')
            mmif = self.app(app, mmif)
            print(f'{app} done')
        self.output_mmif = mmif
        print(f'Finished pipeline of {len(self.pipeline)} apps!')
        self.next(self.end)

    @secrets(sources=['CLAMS-chowda-secret'])
    @kubernetes(
        image='ghcr.io/wgbh-mla/chowda:main',
        persistent_volume_claims={
            'media-pvc': '/m',
        },
    )
    @step
    def end(self):
        """Upload the results to S3 and Chowda, cleanup files"""
        self.s3_path = f'{self.guid}/{self.batch_id}/{self.guid}.mmif'
        self.upload_mmif(self.s3_path)
        self.update_database(self.s3_path)
        self.cleanup()
        print(f'Successfully processed {self.guid}')


if __name__ == '__main__':
    Pipeline()
