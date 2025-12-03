import logging
import sys
from pathlib import Path
from typing import Optional

def setup_logger(
    name: str,
    log_file: Optional[Path] = None,
    level: int = logging.INFO,
    format_str: str = '%(asctime)s - %(levelname)s - %(message)s'
) -> logging.Logger:
    """
    Sets up a logger with the specified configuration.
    
    Args:
        name: Name of the logger.
        log_file: Path to the log file. If None, only stream handler is used.
        level: Logging level.
        format_str: Log format string.
        
    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid adding handlers if they already exist
    if logger.handlers:
        return logger
        
    formatter = logging.Formatter(format_str)
    
    # Stream Handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    # File Handler
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
    return logger