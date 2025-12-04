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
    Crea un logger que se encarga de registrar los eventos en un archivo y en la consola.
    
    Args:
        name: Nombre del logger.
        log_file: Ruta al archivo de log. Si None, solo se usa el handler de consola.
        level: Nivel de logging.
        format_str: Formato de los logs.
        
    Returns:
        Logger configurado.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Evita agregar handlers si ya existen (previene logs duplicados)
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