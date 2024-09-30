#!/usr/bin/env python3

from mario.models import Job
from mario.auth import login
from mario.log import log

# This script is used to run the pipeline for the project
# It will run the following steps:
# 0. Checkout a job from the queue
# 1. Download the file
# 2. Run whisper
# 3. Upload the results to the server
# 4. Cleanup


def checkout_job():
    log.info('Checking out a job from the queue')
    return Job(id=1, media_url='http://media.com/file1.mp4')


def download(job: Job):
    log.info(f'Downloading the file, {job}')


def run_whisper(job: Job):
    log.info('Running whisper', job)


def upload(job: Job):
    log.info(f'Uploading the results to the server {job}')


def cleanup():
    log.info('Cleaning up the pipeline')


def main():
    log.info("It's a me! Mario!")

    login()

    job = checkout_job()

    download(job)

    run_whisper(job)

    upload(job)

    cleanup()

    log.success('Pipeline is complete')


if __name__ == '__main__':
    main()
