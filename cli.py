import click
import logging

import s3lib


def _bytes_to_human(_bytes):
    """Format number of bytes to a readable file size (e.g. 10.1 MB,
    13 kB, etc.)"""
    _bytes = float(_bytes)
    for units in ['B', 'K', 'M', 'G', 'T']:
        if _bytes < 1000.:
            # for anything bigger than bytes round to one decimal point
            # for bytes no decimals
            if units is not 'Bytes':
                return "{:.1f}{}".format(_bytes, units)
            else:
                return "{:.0f}{}".format(_bytes, units)
        _bytes /= 1000.
    # if number of bytes is way too big just use petabytes
    return "{:,.1f}{}".format(_bytes, "PB")


@click.group()
def cli():
    pass


@cli.command()
@click.argument('s3path', type=str, default='')
@click.option('--recursive', '-r', is_flag=True, help="Recursively walk through files.")
@click.option('--human', '-h', is_flag=True, help="Show filesize as human readable format.")
def ls(s3path, recursive, human):
    """List files under s3path"""

    if not s3path:
        files = s3lib.list_buckets()
    else:
        files = s3lib.ls(s3path, recursive=recursive)

    for file in sorted(files):
        owner_name = getattr(file, 'owner_name', '')
        click.echo(owner_name.ljust(15), nl=False)

        size = file.size if hasattr(file, 'size') else None
        if human and size is not None:
            size = _bytes_to_human(size)
        click.echo((str(size) if size is not None else '').rjust(10), nl=False)
        last_modified = getattr(file, 'last_modified', None)

        if last_modified is not None:
            last_modified = last_modified.strftime('%b %d %H:%M')
        else:
            last_modified = ''
        click.echo(last_modified.rjust(16) + ' ', nl=False)

        if isinstance(file, s3lib.s3.S3Directory):
            click.secho(file.path[:-1], fg='blue', nl=False)
            click.echo('/')
        else:
            click.echo(file.path)


@cli.command()
@click.argument('s3path', type=str)
@click.option('--recursive', '-r', is_flag=True, help="Recursively walk through files.")
@click.option('--human', '-h', is_flag=True, help="Show filesize as human readable format.")
def du(s3path, recursive, human):
    """List disk usage (total file size) under s3path"""
    total_bytes = s3lib.du(s3path, recursive=recursive)
    if human:
        total_bytes = _bytes_to_human(total_bytes)
    click.echo(total_bytes)


if __name__ == '__main__':
    cli()
