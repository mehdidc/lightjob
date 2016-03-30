import click

@click.group()
def main():
    pass

@click.command()
def init():
    pass

main.add_command(init)
