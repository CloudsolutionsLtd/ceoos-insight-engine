import logging
import sys

def setup_logging(level="INFO"):
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

def get_logger(name):
    return logging.getLogger(name)