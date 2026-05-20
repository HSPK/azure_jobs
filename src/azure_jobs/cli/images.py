from __future__ import annotations

import click

from azure_jobs.cli import main


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
    """List available Singularity base images.

    Queries the Singularity API for curated images that can be used
    in environment.sing.yaml with the amlt-sing/ prefix.
    """
    from azure_jobs.utils.ui import console, show_sing_images_table

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
    """Fetch Singularity base images via Azure ARM REST API."""
    import logging

    from azure_jobs.core.az_client import AzureARMClient

    log = logging.getLogger(__name__)
    with AzureARMClient() as arm:
        try:
            subs = arm.list_subscriptions()
        except Exception:
            log.debug("list_subscriptions failed", exc_info=True)
            return []
        for sub_id in subs:
            try:
                data = arm.get(
                    f"https://management.azure.com/subscriptions/{sub_id}"
                    f"/providers/Microsoft.Singularity/images"
                    f"?api-version=2020-12-01-preview"
                )
                if data and data.get("value"):
                    return _parse_images(data["value"])
            except Exception:
                log.debug(
                    "Singularity images fetch failed for %s", sub_id, exc_info=True
                )
                continue
    return []


def _parse_images(raw_images: list[dict]) -> list[dict]:
    """Parse raw Singularity API response into structured image list."""
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
