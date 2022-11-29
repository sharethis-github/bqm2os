"""
Create a manifest file (see structure below)

{
    "entries": [
        {
            "meta": { "content_length": 333 },
            "mandatory": true,
            "url": "s3://gcsbucket/gcspathtofile"
        },
    ]
}
"""
import argparse
import json

from google.cloud import storage


def split_uri(uri: str = None):
    """
    Splitting a uri of the from gs://bucket/path/
    """
    tail = uri[5:]
    parts = tail.split("/")
    bucket = parts.pop(0)
    prefix = "/".join(parts)
    return bucket, prefix


def list_blobs(bucket: str, prefix: str):
    """
    Given a full gs uri, list all files under that particular prefix
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_or_name=bucket)
    blobs = bucket.list_blobs(prefix=prefix)
    return blobs


def create_entries(blobs, suffix):
    out = {"entries": []}
    for blob in blobs:
        if blob.endswith(suffix):
            entry = {
                "meta": {
                    "content_length": 1
                },
                "mandatory": True,
                "url": f"s3://bucket/{blob}"
            }
            out["entries"].append(entry)
    return out


def generate_manifest(
    data_path: str = None,
    filematch_suffix: str = None,
    manifest_path: str = None,
    dry_run: bool = True,
):
    """
    DO NOT write to stdout (or print) any data besides
    the manifest json in the form of a string
    """
    output_dict = {
        "entries": []
    }

    data_bucket, data_prefix = split_uri(data_path)
    manifest_bucket, manifest_prefix = split_uri(manifest_path)

    if manifest_prefix.endswith("/"):
        raise Exception(
            f"""
            The manifest-path argument provided
            ({manifest_prefix}) cannot end with a trailing slash
            """
        )

    blobs = list_blobs(bucket=data_bucket, prefix=data_prefix)
    for blob in blobs:
        if blob.name.endswith(filematch_suffix):
            entry = {
                "meta": {
                    "content_length": blob.size
                },
                "mandatory": True,
                "url": f"s3://{data_bucket}/{blob.name}"
            }
            output_dict["entries"].append(entry)

    if not len(output_dict["entries"]):
        raise Exception(
            f"""
            No files matching the suffix {filematch_suffix}
            under the prefix {data_prefix}
            """
        )

    # create a manifest string from the dictionary above
    output_dict_string = json.dumps(output_dict, sort_keys=True)
    if not dry_run:
        # put a file called manifest on gcs bucket in manifest path provided
        client = storage.Client()
        bucket_object = client.get_bucket(manifest_bucket)
        blob = bucket_object.blob(f"{manifest_prefix}")
        # use blob.upload_from_string
        blob.upload_from_string(output_dict_string)
    print(output_dict_string)


if __name__ == "__main__":
    """
    DO NOT write to stdout (or print) any data besides
    the manifest json in the form of a string

    - The data-path MUST specify a full gs uri
    - The filematch-suffix does NOT support wildcards
    - The manifest-path MUST specify a gs uri ending in manifest
    (no trailing slash) (s3://bucket/prefix/manifest)
    """
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("--data-path", type=str, required=True)
    argument_parser.add_argument("--filematch-suffix", type=str, required=True)
    argument_parser.add_argument("--manifest-path", type=str, required=True)
    argument_parser.add_argument("--dry-run", action='store_true')

    args = argument_parser.parse_args()

    generate_manifest(
        args.data_path,
        args.filematch_suffix,
        args.manifest_path,
        args.dry_run
    )
