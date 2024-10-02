#!/usr/bin/env python3

from mario.models import Job
from mario.auth import login
from mario.log import log


def create_pipeline(model_id: str = "ylacombe/whisper-large-v3-turbo"):
    import torch
    from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor, pipeline

    log.info('Creating the pipeline')
    device = "cuda:0" if torch.cuda.is_available() else "cpu"
    torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32

    model = AutoModelForSpeechSeq2Seq.from_pretrained(
        model_id, torch_dtype=torch_dtype, low_cpu_mem_usage=True, use_safetensors=True
    )
    model.to(device)
    processor = AutoProcessor.from_pretrained(model_id)

    pipe = pipeline(
        "automatic-speech-recognition",
        model=model,
        tokenizer=processor.tokenizer,
        feature_extractor=processor.feature_extractor,
        torch_dtype=torch_dtype,
        device=device,
        return_timestamps=True,
    )
    log.info('Pipeline created successfully')
    return pipe


def checkout_job():
    log.info('Checking out a job from the queue')
    return Job(id=1, media_url='http://media.com/file1.mp4')


def upload(job: Job, result: dict):
    log.info(f'Uploading the results to the server {job}')


def main():
    log.info("It's a me! Mario!")

    pipe = create_pipeline()

    login()

    job = checkout_job()

    result = pipe(job.media_url)

    upload(job, result)

    log.success('Pipeline is complete')


if __name__ == '__main__':
    main()
