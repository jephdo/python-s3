import click
import logging

import s3lib


@click.group()
def cli():
    pass


@cli.command()
@click.argument('s3path', type=str)
@click.option('--recursive', is_flag=True, help="Recursively walk through files.")
@click.option('--human', is_flag=True, help="Show filesize as human readable format.")
def ls(s3path, recursive, human):
    """List files under s3path"""
    for file in s3lib.ls(s3path, recursive=recursive):
        click.echo(file.path)


@cli.command()
@click.argument('s3path', type=str)
@click.option('--recursive', is_flag=True, help="Recursively walk through files.")
@click.option('--human', is_flag=True, help="Show filesize as human readable format.")
def du(s3path, recursive, human):
    """List disk usage (total file size) under s3path"""
    total_bytes = s3lib.du(s3path, recursive=recursive)
    if human:
        total_bytes = s3lib.s3._bytes_to_human(total_bytes)
    click.echo(total_bytes)


if __name__ == '__main__':
    cli()