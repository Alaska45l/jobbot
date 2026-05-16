"""Command-line interface for JobBot."""

from __future__ import annotations

import argparse
import asyncio

from rich.console import Console

from jobbot.core.orchestrator import MAX_PLAYWRIGHT, _async_main


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="jobbot",
        description=(
            "JobBot v2.6 — OSINT, scraping Productor-Consumidor y cold email."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            "  jobbot --dork\n"
            "  jobbot --scrape --concurrencia 2\n"
            "  jobbot --dork-scrape --concurrencia 2\n"
            "  jobbot --mail --min-score 60 --dry-run\n"
            "  jobbot --auto\n"
        ),
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dork",
        action="store_true",
        help="Solo dorking (semillas a DB, sin scraping)",
    )
    mode.add_argument(
        "--scrape",
        action="store_true",
        help="Solo scraping (dominios desde DB)",
    )
    mode.add_argument(
        "--dork-scrape",
        action="store_true",
        dest="dork_scrape",
        help="Dork+Scrape en paralelo (Productor-Consumidor)",
    )
    mode.add_argument("--mail", action="store_true")
    mode.add_argument("--auto", action="store_true")
    mode.add_argument("--wa", action="store_true")

    parser.add_argument(
        "--rubros-file",
        type=str,
        default=None,
        dest="rubros_file",
        metavar="FILE",
    )
    parser.add_argument("--limite-dork", type=int, default=30, dest="limite_dork")
    parser.add_argument(
        "--concurrencia",
        type=int,
        default=2,
        help=f"Instancias Playwright (máx {MAX_PLAYWRIGHT} por RAM)",
    )
    parser.add_argument("--min-score", type=int, default=55, dest="min_score")
    parser.add_argument("--dry-run", action="store_true", dest="dry_run")
    parser.add_argument("--limite", type=int, default=10)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument(
        "--forzar-rescraping",
        action="store_true",
        dest="forzar_rescraping",
    )
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.dry_run and not (args.mail or args.auto or args.wa):
        parser.error("--dry-run solo tiene efecto con --mail, --wa o --auto")
    if not (1 <= args.concurrencia <= 10):
        parser.error("--concurrencia debe estar entre 1 y 10")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(parser, args)

    err = Console(stderr=True)
    try:
        asyncio.run(_async_main(args))
    except asyncio.CancelledError:
        err.print(
            "\n[bold yellow]Ejecución cancelada. DB consistente (WAL). "
            "No quedan procesos Chromium huérfanos.[/bold yellow]"
        )
    except KeyboardInterrupt:
        err.print(
            "\n[bold yellow]Interrumpido. DB consistente (WAL). "
            "No quedan procesos Chromium huérfanos.[/bold yellow]"
        )
    except EnvironmentError as exc:
        err.print(f"\n[bold red]Error de configuración:[/bold red] {exc}")
        raise SystemExit(1)
    except ImportError as exc:
        err.print(f"\n[bold red]Dependencia faltante:[/bold red] {exc}")
        raise SystemExit(1)
    except Exception as exc:
        err.print(f"\n[bold red]Error fatal:[/bold red] {exc}")
        raise SystemExit(1)
