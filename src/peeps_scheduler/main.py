import argparse
import logging
import sys
from datetime import date
from pathlib import Path
from peeps_scheduler import constants, utils
from peeps_scheduler.adapters.file.loader import (
    DEFAULT_BASE_PATH,
    load_period_from_files,
)
from peeps_scheduler.adapters.file.validation import PeriodValidationError
from peeps_scheduler.availability_report import run_availability_report
from peeps_scheduler.models import PeriodData
from peeps_scheduler.scheduler import Scheduler


def _load_period_data_from_files(
    period_slug: str,
    base_path: Path | None = None,
    year: int | None = None,
    require_responses: bool = True,
    require_attendance: bool = False,
) -> PeriodData:
    """Load period datafor the given period slug and base path using FilePeriodLoader."""
    if base_path is None:
        base_path = DEFAULT_BASE_PATH
    if year is None:
        year = date.today().year

    try:
        period_data = load_period_from_files(
            period_path=base_path / period_slug,
            year=year,
            require_responses=require_responses,
            require_attendance=require_attendance,
        )
    except (PeriodValidationError, FileNotFoundError) as exc:
        logging.error(str(exc))
        sys.exit(1)

    return period_data


def apply_results(
    period_slug: str,
    base_path: Path | None = None,
    year: int | None = None,
):
    apply_results_data = _load_period_data_from_files(
        period_slug=period_slug,
        base_path=base_path,
        year=year,
        require_responses=False,
        require_attendance=True,
    )

    scheduler = Scheduler(
        period_data=apply_results_data,
        data_folder=period_slug,
    )
    scheduler.apply_results()


def run_scheduler(
    period_slug: str,
    base_path: Path | None = None,
    year: int | None = None,
    max_events: int = constants.DEFAULT_MAX_EVENTS,
):
    period_data = _load_period_data_from_files(
        period_slug=period_slug,
        base_path=base_path,
        year=year,
        require_responses=True,
        require_attendance=False,
    )

    scheduler = Scheduler(
        period_data=period_data,
        data_folder=period_slug,
        max_events=max_events,
    )
    scheduler.run_scheduler()


def availability_report(
    period_slug: str,
    base_path: Path | None = None,
    year: int | None = None,
):
    period_data = _load_period_data_from_files(
        period_slug=period_slug,
        base_path=base_path,
        year=year,
        require_responses=True,
        require_attendance=False,
    )
    run_availability_report(period_data)


def existing_dir(path_str: str) -> Path:
    path = Path(path_str)
    if not path.is_dir():
        raise argparse.ArgumentTypeError(f"'{path_str}' is not a valid directory")
    return path


def main():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--period-slug",
        required=True,
        help="Period slug (e.g., 2026-02; corresponds to folder name under base path)",
    )
    common.add_argument(
        "--base-path",
        required=False,
        type=existing_dir,
        default=None,
        help="Base path for period data files",
    )
    common.add_argument(
        "--year",
        required=False,
        type=int,
        default=None,
        help="Year for the period data (default: current year)",
    )

    parser = argparse.ArgumentParser(description="Peeps Event Scheduler CLI")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose (DEBUG) logging")

    subparsers = parser.add_subparsers(dest="command")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run the scheduler", parents=[common])
    run_parser.add_argument(
        "--max-events",
        type=int,
        default=constants.DEFAULT_MAX_EVENTS,
        help="Maximum number of events to schedule",
    )

    # Apply results command
    _apply_parser = subparsers.add_parser(
        "apply-results", help="Apply actual attendance to update members CSV", parents=[common]
    )

    # Availability report command
    _availability_parser = subparsers.add_parser(
        "availability-report", help="Generate availability report from responses", parents=[common]
    )

    # Pretty results command
    pretty_results_parser = subparsers.add_parser(
        "pretty-results", help="Pretty-print results.json for quick review", parents=[common]
    )

    pretty_results_parser.add_argument(
        "--results-file",
        default="results.json",
        help="Results filename (default: results.json)",
    )

    args = parser.parse_args()
    utils.setup_logging(verbose=args.verbose)

    # Routing logic
    if args.command == "run":
        run_scheduler(
            period_slug=args.period_slug,
            base_path=args.base_path,
            year=args.year,
            max_events=args.max_events,
        )
    elif args.command == "apply-results":
        apply_results(
            period_slug=args.period_slug,
            base_path=args.base_path,
            year=args.year,
        )
    elif args.command == "availability-report":
        availability_report(
            period_slug=args.period_slug,
            base_path=args.base_path,
            year=args.year,
        )
    elif args.command == "pretty-results":
        utils.print_results_summary(args.period_slug, results_filename=args.results_file)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
