import yaml
import os

def load_config():
    config_path = os.environ.get("CONFIG_PATH", "docker.yaml")
    with open(f"/app/internal/config/{config_path}", "r") as f:
        return yaml.safe_load(f) 