import datetime
import itertools
import json
import logging
from peeps_scheduler.constants import DATE_FORMAT, DATESTR_FORMAT
from peeps_scheduler.data_manager import get_data_manager
from peeps_scheduler.file_io import load_csv, normalize_email
from peeps_scheduler.models import Event, EventSequence, Peep, Role


def generate_event_permutations(events):
    """Generates all possible permutations of event sequences as a list of event ids."""

    if not events:
        return []
    event_ids = [event.id for event in events]
    index_sequences = list(itertools.permutations(event_ids, len(event_ids)))

    logging.debug(f"Total permutations: {len(index_sequences)}")
    return index_sequences


def setup_logging(verbose=False):
    stream_log_level = logging.DEBUG if verbose else logging.INFO

    # stream level is set by the verbose arg
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(stream_log_level)

    # file level is alway DEBUG
    file_handler = logging.FileHandler("debug.log")
    file_handler.setLevel(logging.DEBUG)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[stream_handler, file_handler],
    )


def apply_event_results(result_json, members_csv, responses_csv):
    peep_rows = load_csv(members_csv)
    fresh_peeps = []
    for row in peep_rows:
        peep = Peep(
            id=row["id"],
            full_name=row["Name"],
            display_name=row["Display Name"],
            email=row["Email Address"],
            role=row["Role"],
            index=int(row["Index"]),
            priority=int(row["Priority"]),
            total_attended=int(row["Total Attended"]),
            availability=[],
            event_limit=0,
            min_interval_days=0,
            active=row["Active"],
            date_joined=row["Date Joined"],
        )
        fresh_peeps.append(peep)

    # Process responses to mark who responded
    responded_emails = set()
    if responses_csv and responses_csv.exists():
        response_rows = load_csv(responses_csv)
        for row in response_rows:
            email = normalize_email(row.get("Email Address", ""))
            if email:  # Only add non-empty emails
                responded_emails.add(email)
        logging.debug(f"Found {len(responded_emails)} unique respondents in {responses_csv}")
    else:
        logging.debug(
            "No responses file provided or file does not exist; skipping response processing."
        )

    # Set responded flag based on email match
    for peep in fresh_peeps:
        if peep.email and normalize_email(peep.email) in responded_emails:
            peep.responded = True
            logging.debug(f"Marked peep {peep.id} ({peep.email}) as responded")
        else:
            peep.responded = False

    with result_json.open() as f:
        result_data = json.load(f)

    event_data = result_data["valid_events"]
    events = []
    for e in event_data:
        event = Event(
            id=e["id"],
            duration_minutes=e["duration_minutes"],
            date=datetime.datetime.strptime(e["date"], DATE_FORMAT),
            min_role=0,
            max_role=0,
        )
        for peep_info in e["attendees"]:
            for peep in fresh_peeps:
                if peep.id == peep_info["id"]:
                    role = Role.from_string(peep_info["role"])
                    event.add_attendee(peep, role)
        events.append(event)

    sequence = EventSequence(events, fresh_peeps)
    sequence.valid_events = events  # Mark them valid (since they came from results.json)

    # Only update actual attendees, alts are not considered now
    for event in sequence.valid_events:
        Peep.update_event_attendees(fresh_peeps, event)
    sequence.finalize()

    return sequence.peeps


def format_event_date_str(date_str):
    dt = datetime.datetime.strptime(date_str, DATE_FORMAT)
    formatted = dt.strftime(DATESTR_FORMAT)
    formatted = formatted.replace(" 0", " ")
    formatted = formatted[:-2] + formatted[-2:].lower()
    return formatted


def print_results_summary(period_slug, results_filename="results.json"):
    dm = get_data_manager()
    period_path = dm.get_period_path(period_slug)
    results_path = period_path / results_filename

    if not results_path.exists():
        raise FileNotFoundError(f"results.json not found: {results_path}")

    with results_path.open(encoding="utf-8") as f:
        results = json.load(f)

    events = results.get("valid_events", [])

    def _sort_key(event):
        date_str = event.get("date", "")
        try:
            return (0, datetime.datetime.strptime(date_str, DATE_FORMAT))
        except (TypeError, ValueError):
            return (1, date_str or "")

    for event in sorted(events, key=_sort_key):
        date_str = event.get("date", "")
        duration = event.get("duration_minutes")
        topic = event.get("topic")
        leaders = event.get("leaders_string", "")
        followers = event.get("followers_string", "")
        topic_scores = event.get("topic_scores", [])

        try:
            date_label = format_event_date_str(date_str)
        except (TypeError, ValueError):
            date_label = date_str or "Unknown date"

        header = date_label
        if duration:
            header += f", {duration} mins"
        if topic:
            header += f" -> {topic}"
        print(header)

        if leaders:
            print(f"  {leaders}")
        if followers:
            print(f"  {followers}")

        if topic_scores:
            print("  Topic scores:")
            for item in topic_scores:
                topic_name = item.get("topic", "")
                score = item.get("score", 0)
                print(f"    - {topic_name}: {score}")

        print()
