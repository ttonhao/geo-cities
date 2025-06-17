#!/bin/bash
# Script de inicialização do Sistema SQLite

echo "🚀 Iniciando Sistema Avançado de Distâncias com SQLite"

# Configurar ambiente
export STREAMLIT_ENV=development
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
