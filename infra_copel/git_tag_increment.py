import subprocess
import argparse
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

TAG_PREFIX = "v"


def run_command(command):
    """Executes a command and returns the output."""
    try:
        result = subprocess.check_output(
            command, shell=True, universal_newlines=True
        ).strip()
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing command: {command}")
        logging.error(e.output)
        return None


def get_latest_tag():
    """Gets the latest tag from the Git repository."""
    return run_command("git describe --tags --abbrev=0")


def increment_version(version, part):
    """Increments the version based on the chosen part (patch, minor, major)."""
    major, minor, patch = map(int, version.split("."))

    if part == "patch":
        patch += 1
    elif part == "minor":
        minor += 1
        patch = 0
    elif part == "major":
        major += 1
        minor = 0
        patch = 0
    else:
        raise ValueError("Invalid option. Choose 'patch', 'minor', or 'major'.")

    return f"{major}.{minor}.{patch}"


def create_new_tag(new_version, message, test_mode):
    """Creates a new tag in Git with an optional message, skips creation in test mode."""
    new_tag = f"{TAG_PREFIX}{new_version}"
    tag_message = message if message else new_tag

    if test_mode:
        logging.info(
            f"[TEST MODE] New tag would be created: {new_tag} with message: {tag_message}"
        )
    else:
        run_command(f"git tag -a {new_tag} -m '{tag_message}'")
        logging.info(f"New tag created: {new_tag} with message: {tag_message}")

    return new_tag


def pull_tags():
    """Pulls tags from the remote repository."""
    run_command("git pull origin --tags")
    logging.info("Tags successfully updated.")


def push_tags():
    """Pushes tags to the remote repository."""
    run_command("git push origin --tags")
    logging.info("Tags pushed to the remote repository.")


def main():
    parser = argparse.ArgumentParser(
        description="Increments the project version (patch, minor, major)."
    )
    parser.add_argument(
        "version_part",
        nargs="?",  # This makes the argument optional
        default="patch",  # Default value is "patch"
        choices=["patch", "minor", "major"],
        help="Which part of the version do you want to increment: patch (default), minor, or major",
    )
    parser.add_argument(
        "-m",
        "--message",
        help="Custom message for the Git tag. If not provided, the tag name will be used as the message.",
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",  # This makes the argument a flag
        help="Test mode. Simulates the tag creation without executing the Git commands.",
    )

    args = parser.parse_args()

    # Pull the tags
    pull_tags()

    latest_tag = get_latest_tag()

    if latest_tag:
        logging.info(f"Latest tag found: {latest_tag}")
        current_version = latest_tag.lstrip(TAG_PREFIX)
    else:
        logging.info("No tag found. Using the initial version v0.0.0.")
        current_version = "0.0.0"

    try:
        new_version = increment_version(current_version, args.version_part)
    except ValueError as e:
        logging.error(e)
        return

    # Create the new tag, check if in test mode
    new_tag = create_new_tag(new_version, args.message, args.test)

    # Push the tags only if not in test mode
    if not args.test:
        push_tags()
    else:
        logging.info("[TEST MODE] Tags would be pushed to the remote repository.")


if __name__ == "__main__":
    main()
