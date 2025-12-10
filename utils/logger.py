import logging
import sys
from pathlib import Path
from typing import Optional

def setup_logger(
    name: str,
    log_file: Optional[Path] = None,
    level: int = logging.INFO,
    format_str: str = '%(asctime)s - %(levelname)s - %(message)s',
    capture_external_loggers: Optional[list[str]] = None
) -> logging.Logger:
    """    
    Crea un logger que se encarga de registrar los eventos en un archivo y en la consola.
    
    Args:
        name: Nombre del logger.
        log_file: Ruta al archivo de log. Si None, solo se usa el handler de consola.
        level: Nivel de logging.
        format_str: Formato de los logs.
        capture_external_loggers: Lista de nombres de otros loggers (ej: ['dlt']) 
                                  que usarán los mismos handlers.
        
    Returns:
        Logger configurado.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Evita agregar handlers si ya existen (previene logs duplicados)
    if logger.handlers:
        return logger
        
    formatter = logging.Formatter(format_str)
    
    handlers = []
    
    # Stream Handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    handlers.append(stream_handler)
    
    # File Handler
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
        
    # Asignar handlers al logger principal
    for h in handlers:
        logger.addHandler(h)

    # Configurar loggers externos (como dlt) para usar los mismos handlers
    if capture_external_loggers:
        for ext_name in capture_external_loggers:
            ext_logger = logging.getLogger(ext_name)
            ext_logger.setLevel(level)
            # Evitamos duplicar si ya tienen handlers (opcional, depende comportamiento deseado)
            if not ext_logger.handlers:
                for h in handlers:
                    ext_logger.addHandler(h)
                # Evitar propagación para no duplicar en root logger si este ya imprime
                ext_logger.propagate = False 

    return logger