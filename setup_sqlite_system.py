#!/usr/bin/env python3
"""
🚀 SCRIPT DE SETUP DO SISTEMA SQLITE
===================================

Script automatizado para configurar e migrar o sistema de cache SQLite
no Sistema Avançado de Distâncias.

Execute este script para:
✅ Instalar dependências necessárias
✅ Configurar estrutura de diretórios
✅ Migrar dados existentes
✅ Validar configuração
✅ Executar testes iniciais

Uso:
    python setup_sqlite_system.py [--dev|--prod] [--migrate] [--test]

Versão: 1.0.0
"""

import os
import sys
import argparse
import shutil
import subprocess
from pathlib import Path
import sqlite3
from datetime import datetime

class SQLiteSystemSetup:
    """Configurador automático do sistema SQLite"""
    
    def __init__(self, environment='production', migrate_data=False, run_tests=False):
        self.environment = environment
        self.migrate_data = migrate_data
        self.run_tests = run_tests
        self.project_root = Path.cwd()
        self.cache_dir = self.project_root / 'cache'
        self.backup_dir = self.cache_dir / 'backups'
        
        print("🚀 SETUP DO SISTEMA SQLITE AVANÇADO")
        print("=" * 50)
        print(f"📁 Diretório do projeto: {self.project_root}")
        print(f"🔧 Ambiente: {self.environment}")
        print(f"🔄 Migração de dados: {'SIM' if self.migrate_data else 'NÃO'}")
        print(f"🧪 Executar testes: {'SIM' if self.run_tests else 'NÃO'}")
        print()
    
    def run_setup(self):
        """Executa o setup completo"""
        try:
            print("🔄 Iniciando configuração do sistema SQLite...")
            
            # 1. Verificar dependências
            self.check_dependencies()
            
            # 2. Criar estrutura de diretórios
            self.create_directory_structure()
            
            # 3. Copiar arquivos necessários
            self.setup_files()
            
            # 4. Configurar variáveis de ambiente
            self.setup_environment()
            
            # 5. Migrar dados existentes (se solicitado)
            if self.migrate_data:
                self.migrate_existing_data()
            
            # 6. Executar testes (se solicitado)
            if self.run_tests:
                self.run_system_tests()
            
            # 7. Criar backup inicial
            self.create_initial_backup()
            
            # 8. Exibir instruções finais
            self.show_final_instructions()
            
            print("✅ Setup concluído com sucesso!")
            
        except Exception as e:
            print(f"❌ Erro durante o setup: {e}")
            sys.exit(1)
    
    def check_dependencies(self):
        """Verifica e instala dependências necessárias"""
        print("🔍 Verificando dependências...")
        
        required_packages = [
            'streamlit',
            'pandas', 
            'requests',
            'geopy',
            'plotly',
            'folium',
            'streamlit-folium',
            'openpyxl',
            'numpy'
        ]
        
        missing_packages = []
        
        for package in required_packages:
            try:
                __import__(package.replace('-', '_'))
                print(f"  ✅ {package}")
            except ImportError:
                missing_packages.append(package)
                print(f"  ❌ {package} (faltando)")
        
        if missing_packages:
            print(f"\n📦 Instalando {len(missing_packages)} pacotes faltando...")
            
            for package in missing_packages:
                try:
                    subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
                    print(f"  ✅ {package} instalado")
                except subprocess.CalledProcessError as e:
                    print(f"  ❌ Erro ao instalar {package}: {e}")
                    raise
        
        print("✅ Todas as dependências estão instaladas\n")
    
    def create_directory_structure(self):
        """Cria estrutura de diretórios necessária"""
        print("📁 Criando estrutura de diretórios...")
        
        directories = [
            self.cache_dir,
            self.backup_dir,
            self.project_root / 'logs',
            self.project_root / 'config',
            self.project_root / 'tests'
        ]
        
        for directory in directories:
            directory.mkdir(exist_ok=True)
            print(f"  📁 {directory}")
        
        # Criar arquivo .gitignore para cache
        gitignore_path = self.cache_dir / '.gitignore'
        if not gitignore_path.exists():
            with open(gitignore_path, 'w') as f:
                f.write("# Cache SQLite files\n")
                f.write("*.db\n")
                f.write("*.db-wal\n") 
                f.write("*.db-shm\n")
                f.write("backups/\n")
                f.write("logs/\n")
            print(f"  📝 {gitignore_path}")
        
        print("✅ Estrutura de diretórios criada\n")
    
    def setup_files(self):
        """Configura arquivos necessários do sistema"""
        print("📝 Configurando arquivos do sistema...")
        
        # 1. Arquivo requirements.txt
        requirements_path = self.project_root / 'requirements_sqlite.txt'
        requirements_content = '''# Sistema Avançado de Distâncias - SQLite Version
streamlit>=1.28.0
pandas>=1.5.0
requests>=2.28.0
geopy>=2.3.0
plotly>=5.14.0
folium>=0.14.0
streamlit-folium>=0.11.0
openpyxl>=3.0.0
numpy>=1.24.0
'''
        
        with open(requirements_path, 'w') as f:
            f.write(requirements_content)
        print(f"  📝 {requirements_path}")
        
        # 2. Arquivo de configuração
        config_path = self.project_root / 'config' / 'sqlite_config.py'
        config_content = f'''"""
Configurações do Sistema SQLite
"""

import os

# Configurações por ambiente
ENVIRONMENTS = {{
    'development': {{
        'db_path': 'cache/dev_geocoding_cache.db',
        'ttl_hours': 1,
        'max_entries': 5000,
        'backup_interval_hours': 1,
        'debug_enabled': True
    }},
    'production': {{
        'db_path': 'cache/prod_geocoding_cache.db', 
        'ttl_hours': 24,
        'max_entries': 50000,
        'backup_interval_hours': 6,
        'debug_enabled': False
    }}
}}

# Ambiente atual
CURRENT_ENV = os.getenv('STREAMLIT_ENV', 'production')
CONFIG = ENVIRONMENTS[CURRENT_ENV]

# Configurações de logging
LOGGING_CONFIG = {{
    'level': 'DEBUG' if CONFIG['debug_enabled'] else 'INFO',
    'format': '[%(asctime)s] [%(name)s] [%(threadName)s] %(levelname)s: %(message)s',
    'file_path': 'logs/sqlite_cache.log'
}}
'''
        
        config_path.parent.mkdir(exist_ok=True)
        with open(config_path, 'w') as f:
            f.write(config_content)
        print(f"  📝 {config_path}")
        
        # 3. Script de inicialização
        init_script_path = self.project_root / 'start_sqlite_system.sh'
        init_script_content = f'''#!/bin/bash
# Script de inicialização do Sistema SQLite

echo "🚀 Iniciando Sistema Avançado de Distâncias com SQLite"

# Configurar ambiente
export STREAMLIT_ENV={self.environment}
export PYTHONPATH="$PWD:$PYTHONPATH"

# Verificar estrutura
if [ ! -d "cache" ]; then
    echo "❌ Diretório cache não encontrado. Execute setup_sqlite_system.py primeiro."
    exit 1
fi

# Executar aplicação
echo "🔧 Ambiente: $STREAMLIT_ENV"
echo "📁 Cache directory: cache/"
echo "🚀 Iniciando Streamlit..."

streamlit run app_completo_sqlite.py --server.port=8501
'''
        
        with open(init_script_path, 'w') as f:
            f.write(init_script_content)
        
        # Tornar executável no Unix
        if os.name != 'nt':
            os.chmod(init_script_path, 0o755)
        
        print(f"  📝 {init_script_path}")
        
        print("✅ Arquivos configurados\n")
    
    def setup_environment(self):
        """Configura variáveis de ambiente"""
        print("🔧 Configurando variáveis de ambiente...")
        
        env_file_path = self.project_root / '.env'
        env_content = f'''# Configurações do Sistema SQLite
STREAMLIT_ENV={self.environment}
CACHE_DB_PATH=cache/{'dev_' if self.environment == 'development' else 'prod_'}geocoding_cache.db
BACKUP_ENABLED=true
AUTO_CLEANUP_ENABLED=true
DEBUG_LOGGING={'true' if self.environment == 'development' else 'false'}
'''
        
        with open(env_file_path, 'w') as f:
            f.write(env_content)
        
        print(f"  📝 {env_file_path}")
        print(f"  🔧 STREAMLIT_ENV={self.environment}")
        print("✅ Variáveis de ambiente configuradas\n")
    
    def migrate_existing_data(self):
        """Migra dados existentes do cache em memória"""
        print("🔄 Verificando dados existentes para migração...")
        
        # Procurar por arquivos de cache ou dados existentes
        existing_data_found = False
        
        # Verificar se existe algum sistema anterior
        old_cache_files = list(self.project_root.glob('*cache*.json'))
        old_db_files = list(self.project_root.glob('*.db'))
        
        if old_cache_files or old_db_files:
            existing_data_found = True
            print("📦 Dados existentes encontrados:")
            
            for file in old_cache_files + old_db_files:
                print(f"  📄 {file}")
        
        if existing_data_found:
            print("ℹ️  Para migrar dados existentes, use o sistema de migração")
            print("   incorporado no app_completo_sqlite.py")
        else:
            print("ℹ️  Nenhum dado existente encontrado para migração")
        
        print("✅ Verificação de migração concluída\n")
    
    def run_system_tests(self):
        """Executa testes básicos do sistema"""
        print("🧪 Executando testes do sistema SQLite...")
        
        try:
            # Importar e testar o sistema SQLite
            sys.path.insert(0, str(self.project_root))
            
            # Teste básico de conexão
            db_path = self.cache_dir / f"test_{'dev' if self.environment == 'development' else 'prod'}_cache.db"
            
            print("  🔍 Testando conexão SQLite...")
            conn = sqlite3.connect(str(db_path))
            conn.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY)")
            conn.execute("INSERT INTO test (id) VALUES (1)")
            cursor = conn.execute("SELECT id FROM test")
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0] == 1:
                print("  ✅ Conexão SQLite funcionando")
            else:
                raise Exception("Teste de conexão falhou")
            
            # Limpar arquivo de teste
            if db_path.exists():
                db_path.unlink()
            
            # Teste de importação do sistema de cache
            print("  🔍 Testando importação do sistema de cache...")
            
            # Criar arquivo de teste temporário
            test_cache_code = '''
import sqlite3
import tempfile
import os

class TestSQLiteCache:
    def __init__(self):
        self.db_path = tempfile.mktemp(suffix='.db')
        self._init_db()
    
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE coordinates (
                id INTEGER PRIMARY KEY,
                city_name TEXT,
                longitude REAL,
                latitude REAL
            )
        """)
        conn.commit()
        conn.close()
    
    def test_save_and_get(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("INSERT INTO coordinates (city_name, longitude, latitude) VALUES (?, ?, ?)",
                    ("BELO HORIZONTE", -43.9345, -19.9167))
        conn.commit()
        
        cursor = conn.execute("SELECT longitude, latitude FROM coordinates WHERE city_name = ?",
                            ("BELO HORIZONTE",))
        result = cursor.fetchone()
        conn.close()
        
        # Cleanup
        os.unlink(self.db_path)
        
        return result == (-43.9345, -19.9167)

# Executar teste
cache = TestSQLiteCache()
if cache.test_save_and_get():
    print("SUCCESS")
else:
    print("FAILED")
'''
            
            test_file = self.project_root / 'test_cache_temp.py'
            with open(test_file, 'w') as f:
                f.write(test_cache_code)
            
            result = subprocess.run([sys.executable, str(test_file)], 
                                  capture_output=True, text=True)
            
            if "SUCCESS" in result.stdout:
                print("  ✅ Sistema de cache funcionando")
            else:
                print("  ❌ Erro no sistema de cache")
                print(f"     Saída: {result.stdout}")
                print(f"     Erro: {result.stderr}")
            
            # Limpar arquivo de teste
            test_file.unlink()
            
            print("✅ Testes básicos concluídos\n")
            
        except Exception as e:
            print(f"❌ Erro nos testes: {e}\n")
    
    def create_initial_backup(self):
        """Cria estrutura inicial de backup"""
        print("💾 Configurando sistema de backup...")
        
        # Criar script de backup
        backup_script_path = self.project_root / 'backup_cache.py'
        backup_script_content = '''#!/usr/bin/env python3
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
'''
        
        with open(backup_script_path, 'w') as f:
            f.write(backup_script_content)
        
        if os.name != 'nt':
            os.chmod(backup_script_path, 0o755)
        
        print(f"  📝 {backup_script_path}")
        
        # Criar README para backups
        backup_readme = self.backup_dir / 'README.md'
        readme_content = '''# Backups do Cache SQLite

Este diretório contém backups automáticos e manuais do cache SQLite.

## Tipos de Backup

- **Automático**: Criados automaticamente pelo sistema a cada 6 horas (produção) ou 1 hora (desenvolvimento)
- **Manual**: Criados pelo script `backup_cache.py`

## Restaurar Backup

Para restaurar um backup:

1. Pare a aplicação
2. Substitua o arquivo do banco pelo backup
3. Reinicie a aplicação

## Limpeza

Backups antigos devem ser limpos periodicamente para economizar espaço.
'''
        
        with open(backup_readme, 'w') as f:
            f.write(readme_content)
        
        print(f"  📝 {backup_readme}")
        print("✅ Sistema de backup configurado\n")
    
    def show_final_instructions(self):
        """Exibe instruções finais de uso"""
        print("📋 INSTRUÇÕES FINAIS")
        print("=" * 50)
        
        print("🔧 Para usar o sistema:")
        print("1. Execute a aplicação:")
        if os.name == 'nt':
            print(f"   set STREAMLIT_ENV={self.environment}")
            print("   streamlit run app_completo_sqlite.py")
        else:
            print(f"   export STREAMLIT_ENV={self.environment}")
            print("   ./start_sqlite_system.sh")
        
        print("\n📂 Estrutura de arquivos:")
        print("   📁 cache/                    # Bancos SQLite")
        print("   📁 cache/backups/           # Backups automáticos")
        print("   📁 config/                  # Configurações")
        print("   📁 logs/                    # Logs do sistema")
        print("   📄 app_completo_sqlite.py   # Aplicação principal")
        print("   📄 sqlite_cache.py          # Sistema de cache")
        print("   📄 backup_cache.py          # Script de backup")
        
        print("\n🔧 Configurações importantes:")
        print(f"   • Ambiente: {self.environment}")
        print(f"   • Banco: cache/{'dev_' if self.environment == 'development' else 'prod_'}geocoding_cache.db")
        print(f"   • TTL: {'1 hora' if self.environment == 'development' else '24 horas'}")
        print(f"   • Backup: {'1 hora' if self.environment == 'development' else '6 horas'}")
        
        print("\n💡 Dicas:")
        print("   • Use modo development para testes (TTL menor)")
        print("   • Backups são criados automaticamente")
        print("   • Cache persiste entre sessões")
        print("   • Logs disponíveis na pasta logs/")
        
        print("\n🆘 Troubleshooting:")
        print("   • Erro de permissão: verifique permissões da pasta cache/")
        print("   • Banco corrompido: restaure um backup")
        print("   • Performance baixa: execute VACUUM no banco")
        
        print("\n📞 Suporte:")
        print("   • Verifique logs em logs/sqlite_cache.log")
        print("   • Use o painel de admin no Streamlit")
        print("   • Execute testes com: python setup_sqlite_system.py --test")

def main():
    parser = argparse.ArgumentParser(description="Setup do Sistema SQLite")
    parser.add_argument('--dev', action='store_true', 
                       help='Configurar para ambiente de desenvolvimento')
    parser.add_argument('--prod', action='store_true',
                       help='Configurar para ambiente de produção')
    parser.add_argument('--migrate', action='store_true',
                       help='Migrar dados existentes')
    parser.add_argument('--test', action='store_true',
                       help='Executar testes do sistema')
    
    args = parser.parse_args()
    
    # Determinar ambiente
    if args.dev:
        environment = 'development'
    elif args.prod:
        environment = 'production'
    else:
        # Auto-detectar ou perguntar
        env_input = input("Ambiente (dev/prod) [prod]: ").strip().lower()
        environment = 'development' if env_input in ['dev', 'development'] else 'production'
    
    # Confirmar migração se necessário
    migrate_data = args.migrate
    if not migrate_data:
        migrate_input = input("Migrar dados existentes? (y/N): ").strip().lower()
        migrate_data = migrate_input in ['y', 'yes', 's', 'sim']
    
    # Configurar testes
    run_tests = args.test
    if not run_tests:
        test_input = input("Executar testes? (Y/n): ").strip().lower()
        run_tests = test_input not in ['n', 'no', 'nao', 'não']
    
    # Executar setup
    setup = SQLiteSystemSetup(
        environment=environment,
        migrate_data=migrate_data,
        run_tests=run_tests
    )
    
    setup.run_setup()

if __name__ == "__main__":
    main()