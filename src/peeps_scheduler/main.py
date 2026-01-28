import argparse
import logging
import os
import sys
from pathlib import Path
from peeps_scheduler import constants, utils
from peeps_scheduler.data_manager import get_data_manager
from peeps_scheduler.scheduler import Scheduler
from peeps_scheduler.validation import FileValidationError, load_and_validate_period


def apply_results(period_folder):
    dm = get_data_manager()
    period_path = Path(dm.get_period_path(period_folder))

    folder_name = period_path.name
    try:
        year = int(folder_name[:4])
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"cannot infer year from period folder '{folder_name}' (expected YYYY prefix)"
        ) from exc

    try:
        apply_results_data = load_and_validate_period(
            str(period_path),
            year,
            allow_missing_responses=True,
            require_attendance=True,
        )
    except (FileValidationError, FileNotFoundError) as exc:
        logging.error(str(exc))
        sys.exit(1)

    scheduler = Scheduler(
        period_data=apply_results_data,
        data_folder=period_folder,
    )
    scheduler.apply_results()
    return True


def main():
    # Default from environment if available
    default_data_folder = os.getenv("DATA_FOLDER")

    parser = argparse.ArgumentParser(description="Peeps Event Scheduler CLI")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose (DEBUG) logging")

    subparsers = parser.add_subparsers(dest="command")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run the scheduler")
    run_parser.add_argument(
        "--data-folder",
        type=str,
        default=default_data_folder,
        required=(default_data_folder is None),
        help="Path to data folder",
    )
    run_parser.add_argument(
        "--max-events",
        type=int,
        default=constants.DEFAULT_MAX_EVENTS,
        help="Maximum number of events to schedule",
    )

    # Apply results command
    apply_parser = subparsers.add_parser(
        "apply-results", help="Apply actual attendance to update members CSV"
    )
    apply_parser.add_argument(
        "--period-folder",
        required=True,
        help="Path to period folder containing actual_attendance.json, members.csv, and responses.csv",
    )

    # Availability report command
    availability_parser = subparsers.add_parser(
        "availability-report", help="Generate availability report from responses"
    )
    availability_parser.add_argument(
        "--data-folder",
        type=str,
        default=default_data_folder,
        required=(default_data_folder is None),
        help="Path to data folder",
    )

    # Pretty results command
    pretty_results_parser = subparsers.add_parser(
        "pretty-results", help="Pretty-print results.json for quick review"
    )
    pretty_results_parser.add_argument(
        "--period-folder",
        required=True,
        help="Period slug (e.g., 2026-02)",
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
        dm = get_data_manager()
        period_path = Path(dm.get_period_path(args.data_folder))
        folder_name = period_path.name
        try:
            year = int(folder_name[:4])
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"cannot infer year from data folder '{folder_name}' (expected YYYY prefix)"
            ) from exc

        try:
            period_data = load_and_validate_period(str(period_path), year)
        except FileValidationError as exc:
            logging.error(str(exc))
            sys.exit(1)

        scheduler = Scheduler(
            period_data=period_data,
            data_folder=args.data_folder,
            max_events=args.max_events,
        )
        scheduler.run()
    elif args.command == "apply-results":
        apply_results(args.period_folder)
    elif args.command == "availability-report":
        from peeps_scheduler.availability_report import run_availability_report

        run_availability_report(args.data_folder)
    elif args.command == "pretty-results":
        utils.print_results_summary(args.period_folder, results_filename=args.results_file)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
