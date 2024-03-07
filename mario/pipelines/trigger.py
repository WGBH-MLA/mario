from metaflow import FlowSpec, Parameter, step, trigger


@trigger(event='ampersand')
class TriggerPipeline(FlowSpec):
    guid = Parameter('guid', help='GUID of the transcript to process')
    pipeline = Parameter(
        'pipeline', help='Testing "&" handling in flow parameters', separator=','
    )

    @step
    def start(self):
        print(f'Checking {self.pipeline} for "&" handling')
        self.next(self.end)

    @step
    def end(self):
        print('Done!')


if __name__ == '__main__':
    TriggerPipeline()
