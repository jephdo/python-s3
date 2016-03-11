# problems if ls has more than 1k objects
# then disk usage is wrong because it only does 1k objects

#i'm double counting in disk usage somewhere
import os
import re
import collections
import logging
import pathlib

import asyncio
import aiobotocore
import botocore

__all__ = ['ls', 'du', 'get', 'put', 'bucket_and_key_from_path']

logger = logging.getLogger(__name__)


def _bytes_to_human(_bytes):
    """Format number of bytes to a readable file size (e.g. 10.1 MB,
    13 kB, etc.)"""
    _bytes = float(_bytes)
    for units in ['Bytes', 'kB', 'MB', 'GB', 'TB']:
        if _bytes < 1000.:
            # for anything bigger than bytes round to one decimal point
            # for bytes no decimals
            if units is not 'Bytes':
                return "{:.1f} {}".format(_bytes, units)
            else:
                return "{:.0f} {}".format(_bytes, units)
        _bytes /= 1000.
    # if number of bytes is way too big just use petabytes
    return "{:,.1f}{}".format(_bytes, "PB")


def _partition(pred, iterable):
    trues = []
    falses = []
    for item in iterable:
        if pred(item):
            trues.append(item)
        else:
            falses.append(item)
    return trues, falses


def bucket_and_key_from_path(s3path):
    """Returns the bucket and key as a tuple from an S3 filepath."""
    m = re.compile("s3://([^/]+)/(.*)").match(s3path)
    if m:
        return (m.group(1), m.group(2))
    raise ValueError("Not recognizable S3 path '%s'" % s3path)


def get_client():
    session = botocore.session.get_session()
    return session.create_client('s3')


def get_aioclient(loop=None):
    session = aiobotocore.get_session(loop=loop)
    return session.create_client('s3')


def ls(s3path, delimiter='/', recursive=False):
    client = get_aioclient()
    return (asyncio.get_event_loop()
                      .run_until_complete(list_files(client, s3path, delimiter)))


def du(s3path, delimiter='/', recursive=False):
    objects = ls(s3path, delimiter, recursive=False)

    files, directories = _partition(lambda x: isinstance(x, S3File), objects)
    total_size = sum(f.size for f in files)

    if recursive:
        client = get_aioclient()
        for _dir in directories:
            logger.debug("Adding %s to queue of directories to search" % _dir.path)
        subdirs = [disk_usage(client, _dir.path, delimiter) for _dir in directories]
        coroutines = asyncio.gather(*subdirs)
        subdirectory_sizes = (asyncio.get_event_loop()
                                     .run_until_complete(coroutines))
        total_size += sum(subdirectory_sizes)

    return total_size


def get():
    pass


def put():
    pass



async def disk_usage(client, s3path, delimiter='/', total_size=0, queue=None):
    if queue is None:
        queue = collections.deque()
    
    objects = await list_files(client, s3path, delimiter)
    
    files, directories = _partition(lambda x: isinstance(x, S3File), objects)
    

    for _dir in directories:
        logger.debug("Adding %s to queue of directories to search" % _dir.path)
        queue.append(_dir.path)

    while queue:
        _dir = queue.pop()
        total_size += await disk_usage(client, _dir, delimiter=delimiter, queue=queue)
    total_size += sum(f.size for f in files)
    return total_size


async def list_files(client, s3path, delimiter='/'):
    bucket, prefix = bucket_and_key_from_path(s3path)
    objects = await client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)
    
    # if objects['IsTruncated']:
    #     raise ValueError("Returned more than (%s) objects. Increase max keys argument" 
    #         % objects['MaxKeys'])
    # if objects['ResponseMetadata']['HTTPStatusCode'] != 200:
    #     raise ValueError
    bucket = objects['Name']
    directories = [S3Directory(bucket, obj['Prefix']) for obj in objects.get('CommonPrefixes', [])]
    files = [S3File.from_dict(bucket, obj) for obj in objects.get('Contents', [])]
    return directories + files


class S3File:

    def __init__(self, bucket, key, last_modified, size, storage_class):
        self.bucket = bucket
        self.key = key
        self.last_modified = last_modified
        self.size = size
        self.storage_class = storage_class

    @property
    def path(self):
        return os.path.join("s3://", self.bucket, self.key)

    @property
    def filesize(self):
        return _bytes_to_human(self.size)

    @property
    def filename(self):
        return pathlib.Path(self.path).parts[-1]

    def get(self):
        pass

    def download(self, filename=None):
        pass

    def head(self, lines_to_retrieve=10, chunksize=16384, line_separator='\n'):
        client = get_client()
        response = client.get_object(Bucket=self.bucket, Key=self.key)
    
        content_stream = response['Body']
        accrued_lines = []
        unfinished_line = ''
        while len(accrued_lines) < lines_to_retrieve:
            chunk = content_stream.read(chunksize).decode('utf-8')
            # stream is no longer returning any more bytes which signals
            # end of file
            if not chunk:
                break

            lines = (unfinished_line + chunk).split(line_separator)
            unfinished_line = lines.pop()
            for line in lines:
                accrued_lines.append(line)
        # it's possible that there may be extra bytes leftover so you fetch
        # more than the desired number of lines:
        return accrued_lines[:lines_to_retrieve]

    @classmethod
    def from_dict(cls, bucket, _dict):
        key = _dict['Key']
        last_modified = _dict['LastModified']
        size = _dict['Size']
        storage_class = _dict['StorageClass']
        return cls(bucket, key, last_modified, size, storage_class)

    def __repr__(self):
        path = self.path
        filesize = self.filesize
        last_modified = self.last_modified.strftime('%b %d %H:%M')
        cls_name = self.__class__.__name__
        return """<class {cls_name} {path}, {filesize}, {last_modified}>""".format(**vars())


class S3Directory:
    """Representation of a directory on S3.

    """
    def __init__(self, bucket, prefix):
        self.bucket = bucket
        self.prefix = prefix

    @property
    def path(self):
        return os.path.join("s3://", self.bucket, self.prefix)

    def list(self, recursive=False):
        return ls(self.path, recursive=recursive)

    def download(self, recursive=False):
        pass

    def __repr__(self):
        s3path = self.path
        cls_name = self.__class__.__name__
        return """<class {cls_name} {s3path}>""".format(s3path=self.path, cls_name=cls_name)