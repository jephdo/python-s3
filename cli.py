import click
import logging

import s3lib


@click.group()
def cli():
    pass


@cli.command()
@click.argument('s3path', type=str)
@click.option('--recursive', '-r', is_flag=True, help="Recursively walk through files.")
@click.option('--human', '-h', is_flag=True, help="Show filesize as human readable format.")
def ls(s3path, recursive, human):
    """List files under s3path"""
    for file in s3lib.ls(s3path, recursive=recursive):
        last_modified = file.last_modified if hasattr(file, 'last_modified') else None
        if last_modified is not None:
            last_modified = last_modified.strftime('%b %d %Y %H:%M')
        else:
            last_modified = '-'
        click.echo(last_modified.ljust(20), nl=False)

        size = file.size if hasattr(file, 'size') else None
        if human and size is not None:
            size = s3lib.s3._bytes_to_human(size)
        click.echo((str(size) if size is not None else '-').ljust(10), nl=False)

        if isinstance(file, s3lib.s3.S3Directory):
            click.secho(file.path, fg='blue')
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
        total_bytes = s3lib.s3._bytes_to_human(total_bytes)
    click.echo(total_bytes)


if __name__ == '__main__':
    cli()