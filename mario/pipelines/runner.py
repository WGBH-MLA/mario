#!/usr/bin/env python3

from mario.models import Job

# This script is used to run the pipeline for the project
# It will run the following steps:
# 0. Checkout a job from the queue
# 1. Download the file
# 2. Run whisper
# 3. Upload the results to the server
# 4. Cleanup

def download(job: Job):
    print("Downloading the file", job)

def run_whisper(job: Job):
    print("Running whisper", job)


def main():
    print("Checking out a job from the queue")
    job = Job(id=1, media_url= 'http://media.com/file1.mp4')
    print("Downloading the file")
    download(job)

    run_whisper(job)

    print("Uploading the results to the server")

    print("Pipeline is complete")


if __name__ == "__main__":
    main()