from __future__ import annotations

import click

from azure_jobs.cli import main
from azure_jobs.utils.ui import console


@main.group(name="image")
def image_group() -> None:
    """Manage Singularity base images."""


@image_group.command(name="list")
@click.option(
    "--filter", "-f", "query", default=None,
    help="Filter images by name (e.g. 'torch2.7', 'cuda12')",
)
def image_list(query: str | None) -> None:
    """List available Singularity base images.

    Queries the Singularity API for curated images that can be used
    in environment.sing.yaml with the amlt-sing/ prefix.
    """
    from rich.table import Table

    from azure_jobs.core.config import _az_json

    with console.status("[bold cyan]Fetching base images…[/bold cyan]", spinner="dots"):
        images = _fetch_sing_images()

    if query:
        images = [
            img for img in images
            if query.lower() in img["name"].lower()
            or any(query.lower() in a.lower() for a in img["aliases"])
        ]

    if not images:
        from azure_jobs.utils.ui import warning

        warning("No images found" + (f" matching '{query}'" if query else ""))
        return

    table = Table(
        show_header=True,
        header_style="bold",
        show_lines=False,
        pad_edge=True,
        title="[bold]Singularity Base Images[/bold]",
        title_style="",
    )
    table.add_column("ID", style="dim", no_wrap=True)
    table.add_column("Image", style="highlight", no_wrap=True)
    table.add_column("Aliases", style="dim")

    for img in images:
        aliases = [a for a in img["aliases"] if a != img["name"]]
        table.add_row(
            img["id"],
            f"amlt-sing/{img['name']}",
            ", ".join(aliases[:3]) + ("…" if len(aliases) > 3 else ""),
        )

    console.print()
    console.print(table)
    console.print()
    console.print(f"[dim]{len(images)} images available[/dim]")
    console.print()


def _fetch_sing_images() -> list[dict]:
    """Fetch Singularity base images via Azure Management API."""
    from azure_jobs.core.config import _az_json

    # Find a working subscription
    subs = _az_json(["account", "list", "--query", "[].id", "-o", "json"])
    if not subs:
        return []
    for sub_id in subs:
        try:
            data = _az_json([
                "rest", "--method", "get",
                "--url",
                f"https://management.azure.com/subscriptions/{sub_id}"
                f"/providers/Microsoft.Singularity/images?api-version=2020-12-01-preview",
            ])
            if data and data.get("value"):
                return _parse_images(data["value"])
        except Exception:
            continue
    return []


def _parse_images(raw_images: list[dict]) -> list[dict]:
    """Parse raw Singularity API response into structured image list."""
    images = []
    for entry in raw_images:
        names = entry.get("names", [])
        # Pick the most descriptive name (one with a tag)
        name = next((n for n in names if ":" in n), names[-1] if names else "")
        images.append({
            "id": entry.get("id", ""),
            "name": name,
            "aliases": names,
        })
    # Sort by name
    images.sort(key=lambda x: x["name"])
    return images
