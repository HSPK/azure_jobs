from __future__ import annotations

import click


@click.group()
@click.version_option(package_name="azure_jobs")
def main() -> None:
    pass


# Import command modules to trigger @main.command() registration.
from . import run  # noqa: E402, F401
from . import templates  # noqa: E402, F401
from . import jobs  # noqa: E402, F401
from . import images  # noqa: E402, F401
from . import dashboard  # noqa: E402, F401
from . import config  # noqa: E402, F401
from . import auth  # noqa: E402, F401
from . import workspace  # noqa: E402, F401
from . import experiment  # noqa: E402, F401
from . import quota  # noqa: E402, F401
from . import env  # noqa: E402, F401
from . import ds  # noqa: E402, F401
