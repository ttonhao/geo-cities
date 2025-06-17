#!/usr/bin/env python3
"""
Script de backup manual do cache SQLite
"""

import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

def backup_cache(db_path, backup_dir):
    """Cria backup do cache SQLite"""
    if not os.path.exists(db_path):
        print(f"❌ Banco não encontrado: {db_path}")
        return False
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"cache_backup_manual_{timestamp}.db"
    backup_path = os.path.join(backup_dir, backup_name)
    
    # Garantir que diretório existe
    os.makedirs(backup_dir, exist_ok=True)
    
    try:
        # Backup usando SQLite backup API
        source_conn = sqlite3.connect(db_path)
        backup_conn = sqlite3.connect(backup_path)
        source_conn.backup(backup_conn)
        source_conn.close()
        backup_conn.close()
        
        file_size = os.path.getsize(backup_path)
        print(f"✅ Backup criado: {backup_path}")
        print(f"📊 Tamanho: {file_size / (1024*1024):.2f} MB")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro no backup: {e}")
        return False

if __name__ == "__main__":
    import sys
    
    # Configurações
    cache_dir = "cache"
    backup_dir = os.path.join(cache_dir, "backups")
    
    # Determinar ambiente
    env = os.getenv('STREAMLIT_ENV', 'production')
    db_name = f"{'dev_' if env == 'development' else 'prod_'}geocoding_cache.db"
    db_path = os.path.join(cache_dir, db_name)
    
    print(f"🔄 Criando backup do cache ({env})...")
    print(f"📁 Banco: {db_path}")
    print(f"📁 Backup: {backup_dir}")
    
    if backup_cache(db_path, backup_dir):
        print("✅ Backup concluído com sucesso!")
    else:
        print("❌ Erro no backup!")
        sys.exit(1)
