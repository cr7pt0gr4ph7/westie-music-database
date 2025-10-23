"""Download the necessary data files from HuggingFace."""
from huggingface_hub import snapshot_download
import os

this_dir = os.path.dirname(os.path.realpath(__file__))
dataset_dir = os.path.join(this_dir, "processed_data_huggingface")

snapshot_download(repo_id="westie-data-collective/wcs-music-database-v1",
                  repo_type="dataset",
                  allow_patterns=["*.parquet", "*.csv"],
                  local_dir=dataset_dir)
