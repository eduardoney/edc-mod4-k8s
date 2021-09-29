import requests
import zipfile
import os
import logging
from google.cloud import storage


def download_file(url: str, file_path: str) -> bool:
    """Download a file from url and return true on success"""
    try:
        logging.info("Start download file")

        # Request
        req = requests.get(url, allow_redirects=True)
        # Save file
        open(file_path, "wb").write(req.content)

        logging.info(f"Written file {file_path}")
        return True

    except Exception as e:
        logging.error(f"Failed to download a file:{url}")
        logging.error(e)
        return False


def extract_file(file_path: str) -> str:
    try:
        logging.info("Start extract file")

        # Extract file
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(os.path.dirname(file_path))

        logging.info(
            f"Extracted the files on directory {os.path.dirname(file_path)}")
        return os.path.dirname(file_path)

    except Exception as e:
        logging.error(f"Failed to unzip a file {file_path}")
        logging.error(e)
        return None


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    try:
        logging.info("Start upload file")

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

        logging.info(
            f"File {source_file_name} uploaded to {destination_blob_name}.")

    except Exception as e:
        logging.error(
            f"Failed to upload {source_file_name} to {destination_blob_name}")
        logging.error(e)


if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s:%(levelname)s:%(message)s',  level=logging.INFO)

    url = "https://download.inep.gov.br/microdados/Enade_Microdados/microdados_Enade_2017_portal_2018.10.09.zip"
    file_zip = "./data/microdados_Enade_2017_portal_2018.10.09.zip"
    bucket = "bootcamp-edc"

    if download_file(url=url, file_path=file_zip):
        result = extract_file(file_path=file_zip)

    if result:
        file = f"{result}/3.DADOS/MICRODADOS_ENADE_2017.txt"
        destination_file = "landing/MICRODADOS_ENADE_2017.txt"
        upload_blob(bucket_name=bucket,
                    source_file_name=file,
                    destination_blob_name=destination_file)
