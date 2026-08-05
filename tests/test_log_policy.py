"""The SDK and server share one log ordering policy."""

from azure_jobs.shared.logs import order_log_files, pick_default_log


def test_priority_beats_alphabetical_order() -> None:
    files = [
        "azureml-logs/70_driver_log.txt",
        "user_logs/std_log.txt",
        "logs/amlt_code_runner.log",
    ]
    assert order_log_files(files) == [
        "user_logs/std_log.txt",
        "logs/amlt_code_runner.log",
        "azureml-logs/70_driver_log.txt",
    ]
    assert pick_default_log(files) == "user_logs/std_log.txt"


def test_non_log_artifacts_are_not_selected() -> None:
    files = ["outputs/model.bin", "system/metadata.json", "logs/job.out"]
    assert order_log_files(files) == ["logs/job.out"]
    assert pick_default_log(files) == "logs/job.out"


def test_known_log_prefix_needs_no_extension() -> None:
    assert pick_default_log(["logs/amlt_code_runner"]) == "logs/amlt_code_runner"


def test_no_supported_log_returns_empty() -> None:
    assert pick_default_log(["outputs/model.bin"]) == ""
