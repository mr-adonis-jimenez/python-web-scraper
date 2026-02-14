"""Web scraper orchestrator with CLI support.

Coordinates fetching, parsing, rate limiting, and exporting into a single
pipeline that can be run programmatically or from the command line.
"""
import argparse
import logging
import sys
from typing import Optional

from fetcher import fetch_html
from parser import parse_items, SelectorConfig
from exporter import DataExporter, ExportFormat, export_csv

logger = logging.getLogger(__name__)


def run(
    url: str,
    output: str = "output",
    filename: str = "scraped_data",
    fmt: ExportFormat = ExportFormat.CSV,
    selector_config: Optional[SelectorConfig] = None,
) -> Optional[str]:
    """Run the scraping pipeline: fetch -> parse -> export.

    Args:
        url: Target URL to scrape
        output: Output directory for exported files
        filename: Base filename (without extension)
        fmt: Export format
        selector_config: Custom CSS selector configuration

    Returns:
        Path to the exported file, or None on failure
    """
    logger.info(f"Scraping {url}")

    html = fetch_html(url)
    if not html:
        logger.error(f"Failed to fetch {url}")
        return None

    data = parse_items(html, config=selector_config)
    if not data:
        logger.warning("No items parsed from the page")
        return None

    logger.info(f"Parsed {len(data)} items")

    exporter = DataExporter(output_dir=output)
    result = exporter.export(data, filename, fmt)
    if result:
        logger.info(f"Exported to {result}")
    return result


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Python Web Scraper - fetch, parse, and export data from web pages"
    )
    parser.add_argument("url", help="URL to scrape")
    parser.add_argument(
        "-o", "--output",
        default="output",
        help="Output directory (default: output)",
    )
    parser.add_argument(
        "-f", "--filename",
        default="scraped_data",
        help="Output filename without extension (default: scraped_data)",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "json", "xlsx", "parquet"],
        default="csv",
        help="Export format (default: csv)",
    )
    parser.add_argument(
        "--container",
        default=".item",
        help="CSS selector for item containers (default: .item)",
    )
    parser.add_argument(
        "--fields",
        nargs="*",
        metavar="NAME=SELECTOR",
        help="Field mappings as name=selector pairs (e.g. title=.title price=.price)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    fmt = ExportFormat(args.format)

    selector_config = SelectorConfig(container=args.container)
    if args.fields:
        field_map = {}
        for pair in args.fields:
            if "=" not in pair:
                logger.error(f"Invalid field mapping: {pair} (expected name=selector)")
                sys.exit(1)
            name, selector = pair.split("=", 1)
            field_map[name] = selector
        selector_config.fields = field_map

    result = run(
        url=args.url,
        output=args.output,
        filename=args.filename,
        fmt=fmt,
        selector_config=selector_config,
    )

    if result is None:
        sys.exit(1)


if __name__ == "__main__":
    main()
