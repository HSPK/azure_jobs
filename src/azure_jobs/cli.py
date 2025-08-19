import click


@click.group()
def main():
    pass


@main.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True}
)
@click.option(
    "-t",
    "--template",
    help="Template environment to execute the command",
    default=None,
)
@click.argument("command", nargs=1)
@click.argument("args", nargs=-1)
def run(command, args, template):
    cmdline = " ".join([command] + list(args))
    print(f"You want to execute: {cmdline} with template {template}")


if __name__ == "__main__":
    main()
