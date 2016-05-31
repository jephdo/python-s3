# problems if ls has more than 1k objects
# then disk usage is wrong because it only does 1k objects

#i'm double counting in disk usage somewhere
import os
import re
import collections
import fnmatch
import functools
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


def _is_partial_match(s3path, wildcard_path, delimiter='/'):
    """Return True if the s3path matches a wildcard "partially". That is while
    recursively walking directories, does this path match the wildcard up 
    until its respective directory? E.g.:

    >>> is_partial_match("s3://br-user/jeph/byom/", "s3://br-user/jeph/*/*.txt")
    True
    >>> is_partial_match("s3:/br-user/jeph/", "s3://br-user/jeph/*/*.txt")
    True
    >>> is_partial_match("s3://br-user/byom/hello.tsv", "s3://br-user/jeph/*/*.txt")
    False
    """
    # we need to normalize both paths to end the same by
    # removing trailing slashes from directories and add a 
    # wildcard to the matching path. It's necessary for when we split 
    # the path into parts:
    if s3path.endswith(delimiter):
        s3path = s3path[0:-1]
    if wildcard_path.endswith(delimiter):
        wildcard_path += '*'
    
    s3parts = s3path.replace('s3://', '').split(delimiter)
    wildcardparts = wildcard_path.replace('s3://', '').split(delimiter)
    min_part_length = min(len(s3parts), len(wildcardparts))

    a = 's3://' + delimiter.join(s3parts[:min_part_length])
    b = 's3://' + delimiter.join(wildcardparts[:min_part_length])
    is_match = fnmatch.fnmatch(a, b)
    logger.debug("Matching path (%s)[%s] to (%s)[%s]. %s", s3path, a, wildcard_path, 
        b, "Paths partially match." if is_match else "Paths do not partially match.")
    return is_match


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


def ls(s3path, delimiter='/', matching_path=None, recursive=False):
    if '*' in s3path:
        # if there are wildcards in the s3path then the initial `ls` will 
        # go up until the first wildcard e.g. `s3://br-user/jep*/*.txt` => first 
        # call to ls should be `s3://br-user/jep`
        matching_path = s3path
        recursive = True
        s3path = s3path[:s3path.find('*')]
        logger.debug("There exist wildcards in (%s). Initially listing objects under %s", matching_path, s3path)

    try:
        client = get_aioclient()
        files = (asyncio.get_event_loop()
                        .run_until_complete(list_files(client, s3path, delimiter=delimiter, 
                            recursive=recursive, matching_path=matching_path)))
    finally:
        client.close()

    # this is a final filter to make sure all objects return match the wildcard path
    # the calls to `list_files` will superfluously add directories that partially 
    # match at the time, but we don't want in the final output.
    if matching_path is not None:
        files = [f for f in files if fnmatch.fnmatch(f.path, matching_path)]

    return list(sorted(files))


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


async def list_files(client, s3path, delimiter='/', recursive=False, queue=None, 
                     matching_path=None, page_size=1000):
    bucket, prefix = bucket_and_key_from_path(s3path)
    paginator = client.get_paginator('list_objects')

    async for page in paginator.paginate(Bucket=bucket, Prefix=prefix, 
                                         Delimiter=delimiter, PaginationConfig={'PageSize': page_size}):
        assert page['ResponseMetadata']['HTTPStatusCode'] == 200

        bucket = page['Name']
        directories = [S3Directory(bucket, obj['Prefix']) for obj in page.get('CommonPrefixes', [])]
        files = [S3File.from_dict(bucket, obj) for obj in page.get('Contents', [])]
        if matching_path is not None:
            found_files_and_dirs = [x for x in directories + files if _is_partial_match(x.path, matching_path)]
        else:
            found_files_and_dirs = directories + files
        # print(s3path, len(found_files_and_dirs))

        if recursive:
            if queue is None:
                queue = collections.deque()
            for _dir in directories:
                # this is kind of kludgy that I have to do a partial match again here when 
                # I did it before:
                if matching_path is not None: 
                    if _is_partial_match(_dir.path, matching_path):
                        queue.append(_dir.path)
                else:
                    queue.append(_dir.path)
            while queue:
                _dir = queue.pop()
                found_files_and_dirs += await list_files(client, _dir, delimiter, recursive, queue, matching_path, page_size)

    return found_files_and_dirs


@functools.total_ordering
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

    # async def stream(self, client, chunksize=262144):
    #     content_stream = client.get_object(Bucket=self.bucket, Key=self.key)['Body']

    #     while True:
    #         chunk = await content_stream.read(chunksize)
    #         if not chunk:
    #             break
    #         yield chunk

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

    def __eq__(self, other):
        return self.path == other.path

    def __lt__(self, other):
        if isinstance(other, S3Directory):
            return False
        return self.path < other.path

    def __repr__(self):
        path = self.path
        filesize = self.filesize
        last_modified = self.last_modified.strftime('%b %d %H:%M')
        cls_name = self.__class__.__name__
        return """<class {cls_name} {path}, {filesize}, {last_modified}>""".format(**vars())


@functools.total_ordering
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

    def __eq__(self, other):
        return self.path == other.path

    def __lt__(self, other):
        if isinstance(other, S3File):
            return True
        return self.path < other.path

    def __repr__(self):
        s3path = self.path
        cls_name = self.__class__.__name__
        return """<class {cls_name} {s3path}>""".format(s3path=self.path, cls_name=cls_name)