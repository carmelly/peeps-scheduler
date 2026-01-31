import datetime
import itertools
import logging
from peeps_scheduler.adapters.file.loader import FilePeriodLoader
from peeps_scheduler.constants import DATE_FORMAT, DATESTR_FORMAT
from peeps_scheduler.models import Event, Role


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


def format_event_date_str(date_str):
    dt = datetime.datetime.strptime(date_str, DATE_FORMAT)
    formatted = dt.strftime(DATESTR_FORMAT)
    formatted = formatted.replace(" 0", " ")
    formatted = formatted[:-2] + formatted[-2:].lower()
    return formatted


def print_results_summary(period_slug):
    """Print a summary of results from the results.json file for the given period."""
    loader = FilePeriodLoader()
    period_data = loader.load_period(period_slug)

    events = period_data.results_events
    sorted_events: list[Event] = sorted(events, key=lambda e: e.date)

    for event in sorted_events:
        date_str = event.formatted_date()
        duration = event.duration_minutes
        topic = event.topic
        leaders = event.leaders_string
        followers = event.followers_string
        if not leaders and not followers:
            attendees = event.attendees or []
            leader_names = []
            follower_names = []
            for attendee in attendees:
                name = attendee.name
                role = attendee.role
                if role == Role.LEADER:
                    leader_names.append(str(name))
                elif role == Role.FOLLOWER:
                    follower_names.append(str(name))

            if leader_names:
                leaders = f"Leaders({len(leader_names)}): {', '.join(sorted(leader_names))}"
            if follower_names:
                followers = f"Followers({len(follower_names)}): {', '.join(sorted(follower_names))}"

        header = date_str
        if duration:
            header += f", {duration} mins"
        if topic:
            header += f" -> {topic}"
        print(header)

        if leaders:
            print(f"  {leaders}")
        if followers:
            print(f"  {followers}")

        print()
