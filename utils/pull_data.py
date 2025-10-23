"""Download the necessary data files from HuggingFace."""
from huggingface_hub import snapshot_download
import os

this_dir = os.path.dirname(os.path.realpath(__file__))
dataset_dir = os.path.join(this_dir, "..", "processed_data_huggingface")
source_dataset_dir = os.path.join(this_dir, "..", "unprocessed_data_huggingface")


def is_running_on_streamlit() -> bool:
    """
    Checks whether we are running on the Streamlit Community Cloud.

    This is used to automatically pull the data from HuggingFace
    in that case, because the Streamlit Community Cloud does not
    provide a separate "setup" step for that.
    """
    return "STREAMLIT_RUNNER_FAST_RERUNS" in os.environ


def should_auto_pull_from_huggingface() -> bool:
    """
    Checks whether we should automatically pull the data from HuggingFace.
    """
    return is_running_on_streamlit()\
        or os.environ.get("AUTO_PULL_FROM_HUGGINGFACE", default="false").lower() in ["true", "yes", "1"]


def pull_source_data():
    """Pulls the source dataset from HuggingFace."""
    snapshot_download(repo_id="westie-data-collective/wcs-music-database-source-data",
                      repo_type="dataset",
                      allow_patterns=["*.parquet", "*.csv"],
                      local_dir=source_dataset_dir)

def pull_processed_data():
    """Pulls the processed dataset from HuggingFace."""
    snapshot_download(repo_id="westie-data-collective/wcs-music-database-v1",
                      repo_type="dataset",
                      allow_patterns=["*.parquet", "*.csv"],
                      local_dir=dataset_dir)


def automatically_pull_data_if_needed() -> bool:
    """Automatically pull the datasets from the HuggingFace if needed."""
    if not should_auto_pull_from_huggingface():
        print("Skipping automatic data pull from HuggingFace due to detected environment configuration")
        return False

    print("Automatically pulling data from HuggingFace...")
    pull_processed_data()
    print("Done.")
    return True
