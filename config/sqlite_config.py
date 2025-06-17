"""
Configurações do Sistema SQLite
"""

import os

# Configurações por ambiente
ENVIRONMENTS = {
    'development': {
        'db_path': 'cache/dev_geocoding_cache.db',
        'ttl_hours': 1,
        'max_entries': 5000,
        'backup_interval_hours': 1,
        'debug_enabled': True
    },
    'production': {
        'db_path': 'cache/prod_geocoding_cache.db', 
        'ttl_hours': 24,
        'max_entries': 50000,
        'backup_interval_hours': 6,
        'debug_enabled': False
    }
}

# Ambiente atual
CURRENT_ENV = os.getenv('STREAMLIT_ENV', 'production')
CONFIG = ENVIRONMENTS[CURRENT_ENV]

# Configurações de logging
LOGGING_CONFIG = {
    'level': 'DEBUG' if CONFIG['debug_enabled'] else 'INFO',
    'format': '[%(asctime)s] [%(name)s] [%(threadName)s] %(levelname)s: %(message)s',
    'file_path': 'logs/sqlite_cache.log'
}
