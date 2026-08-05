from __future__ import annotations

import click

from azure_jobs.client.cli import main

@main.group(name="image")
def image_group() -> None:
    """Manage Singularity base images."""

@image_group.command(name="list")
@click.option(
    "--filter",
    "-f",
    "query",
    default=None,
    help="Filter images by name (e.g. 'torch2.7', 'cuda12')",
)
def image_list(query: str | None) -> None:
    """List available Singularity base images."""
    from azure_jobs.client.ui import console, show_sing_images_table

    with console.status("[bold cyan]Fetching base images…[/bold cyan]", spinner="dots"):
        images = _fetch_sing_images()

    if query:
        images = [
            img
            for img in images
            if query.lower() in img["name"].lower()
            or any(query.lower() in a.lower() for a in img["aliases"])
        ]

    show_sing_images_table(images)

def _fetch_sing_images() -> list[dict]:
    from azure_jobs import connect

    with connect() as d:
        return _parse_images([dict(item.raw) for item in d.image.list()])


def _parse_images(raw_images: list[dict]) -> list[dict]:
    images = []
    for entry in raw_images:
        names = entry.get("names", [])
        name = next((n for n in names if ":" in n), names[-1] if names else "")
        images.append(
            {
                "id": entry.get("id", ""),
                "name": name,
                "aliases": names,
            }
        )
    images.sort(key=lambda x: x["name"])
    return images
