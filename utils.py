import logging
import yaml
import os

def get_logger(name: str=None, level: int=logging.INFO, config_path: str='config.yaml') -> logging.Logger:
    with open(config_path, 'r') as file:
        config=yaml.safe_load(file)

    logger=logging.getLogger(name)

    if not logger.hasHandlers():
        stream_handler=logging.StreamHandler()
        stream_formatter=logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

        if config.get('logs_enabled', True):
            logs_path=config.get('logs_path', 'logs/rss_feed_logs.log')
            os.makedirs(os.path.dirname(logs_path), exist_ok=True)
            file_handler=logging.FileHandler(logs_path)
            file_formatter=logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

    logger.setLevel(level)
    return logger
