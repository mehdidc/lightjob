import click

@click.group()
def cli():
    pass

@click.command()
def init():
    pass

cli.add_command(init)
