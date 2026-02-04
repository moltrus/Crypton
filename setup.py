import os
import yaml

def load_config() -> dict:
    config_file = "config.yaml"
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Failed to load config from {config_file}: {e}")
        return {}

def create_directories(config: dict):
    data_path = config.get("data_path", "data/")
    logs_path = config.get("logs_path", "data/logs/rss_feed_logs.log")
    sql_db_path = config.get("sql_db", "data/sql_db/db.sqlite3")
    sites_output = config.get("sites", {}).get("output_folder", "rss_feeds")
    gnews_output = config.get("gnews", {}).get("output_folder", "rss_feeds")

    os.makedirs(data_path, exist_ok=True)
    print(f"Created directory: {data_path}")

    logs_dir = os.path.dirname(logs_path)
    os.makedirs(logs_dir, exist_ok=True)
    print(f"Created directory: {logs_dir}")

    sql_dir = os.path.dirname(sql_db_path)
    os.makedirs(sql_dir, exist_ok=True)
    print(f"Created directory: {sql_dir}")

    rss_feeds_path = os.path.join(data_path, sites_output)
    os.makedirs(rss_feeds_path, exist_ok=True)
    print(f"Created directory: {rss_feeds_path}")

    if gnews_output != sites_output:
        gnews_path = os.path.join(data_path, gnews_output)
        os.makedirs(gnews_path, exist_ok=True)
        print(f"Created directory: {gnews_path}")

    sites_urls = config.get("sites", {}).get("urls", [])
    for url in sites_urls:

        from urllib.parse import urlparse
        domain = urlparse(url).netloc
        if domain:
            site_dir = os.path.join(rss_feeds_path, domain)
            os.makedirs(site_dir, exist_ok=True)
            print(f"Created directory: {site_dir}")

def main():
    config = load_config()
    if not config:
        print("No config loaded, exiting.")
        return
    create_directories(config)
    print("Setup complete.")

if __name__ == "__main__":
    main()