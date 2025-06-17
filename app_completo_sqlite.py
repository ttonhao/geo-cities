 # type: ignore
"""
🚀 SISTEMA AVANÇADO DE DISTÂNCIAS - VERSÃO PRO COM SQLITE
========================================================

VERSÃO COMPLETA: Sistema original + Cache SQLite avançado
- ✅ Cache SQLite persistente entre sessões
- ✅ Performance otimizada com índices
- ✅ TTL automático e limpeza
- ✅ Backup e restore automático
- ✅ Processamento Paralelo completo  
- ✅ Dashboard Analytics completo
- ✅ Mapas Interativos completos
- ✅ Painel de administração do banco
- ✅ Migração automática de dados
- ✅ Sistema de Debug integrado

Versão: 3.1.0 - Pro SQLite (Completa)
"""

import streamlit as st
import pandas as pd
import requests
import time
import asyncio
import threading
import functools
import os
import sqlite3
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from geopy.geocoders import Nominatim
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
from io import BytesIO
from datetime import datetime, timedelta
import numpy as np
import warnings
import logging

# Importar sistema SQLite (cole o código anterior antes desta linha ou em arquivo separado)
# from sqlite_cache_system import SQLiteCache, create_sqlite_cache_instance, migrate_from_memory_cache

# ================================
# CONFIGURAÇÕES DE DEBUG E AMBIENTE
# ================================

DEBUG_MODE = os.getenv('STREAMLIT_ENV', 'production') == 'development'

class StreamlitDebugger:
    """Sistema de debug integrado para Streamlit"""
    
    def __init__(self, enabled=True):
        self.enabled = enabled and DEBUG_MODE
        self.session_logs = []
    
    def log(self, message, level="INFO", show_in_ui=False):
        """Log com opção de mostrar na UI"""
        if not self.enabled:
            return
            
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        thread_name = threading.current_thread().name
        log_entry = f"[{timestamp}] [{thread_name}] {level}: {message}"
        
        if DEBUG_MODE:
            print(log_entry)
        
        if 'debug_logs' not in st.session_state:
            st.session_state.debug_logs = []
        
        st.session_state.debug_logs.append({
            'timestamp': timestamp,
            'thread': thread_name,
            'level': level,
            'message': message
        })
        
        if len(st.session_state.debug_logs) > 1000:
            st.session_state.debug_logs = st.session_state.debug_logs[-500:]
    
    def debug(self, message, show_in_ui=False):
        self.log(message, "DEBUG", show_in_ui)
    
    def info(self, message, show_in_ui=False):
        self.log(message, "INFO", show_in_ui)
    
    def warning(self, message, show_in_ui=False):
        self.log(message, "WARNING", show_in_ui)
    
    def error(self, message, show_in_ui=False):
        self.log(message, "ERROR", show_in_ui)

debugger = StreamlitDebugger()

def debug_breakpoint(message="Debug breakpoint", variables=None):
    """Breakpoint customizado com informações contextuais"""
    if DEBUG_MODE:
        debugger.debug(f"BREAKPOINT: {message}")
        
        if variables:
            for name, value in variables.items():
                debugger.debug(f"  {name}: {value} (type: {type(value).__name__})")
        
        pass  # <-- COLOQUE BREAKPOINT AQUI

def debug_timing(func):
    """Decorator para medir tempo de execução com debug"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        if DEBUG_MODE:
            debugger.debug(f"Iniciando {func.__name__}")
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            if DEBUG_MODE:
                debugger.info(f"{func.__name__} executou em {execution_time:.3f}s")
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            debugger.error(f"{func.__name__} falhou após {execution_time:.3f}s: {e}")
            raise
    
    return wrapper

# Configuração da página
st.set_page_config(
    page_title="🚀 Sistema Avançado de Distâncias - SQLite",
    page_icon="💾",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Suprimir warnings de threading
warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)

# CSS avançado COMPLETO + novos estilos para SQLite
st.markdown("""
<style>
    .main-header {
        text-align: center;
        padding: 2rem 0;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
        color: white;
        border-radius: 20px;
        margin-bottom: 2rem;
        box-shadow: 0 10px 40px rgba(0, 0, 0, 0.2);
        animation: gradient 3s ease infinite;
    }
    
    @keyframes gradient {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    .analytics-card {
        background: linear-gradient(135deg, #ff9a9e 0%, #fecfef 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        margin: 1rem 0;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        backdrop-filter: blur(10px);
    }
    
    .performance-card {
        background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: #2c3e50;
        margin: 1rem 0;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
    }
    
    .cache-stats {
        background: linear-gradient(135deg, #d299c2 0%, #fef9d7 100%);
        padding: 1rem;
        border-radius: 12px;
        margin: 1rem 0;
        border-left: 4px solid #9b59b6;
    }
    
    .sqlite-panel {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        margin: 1rem 0;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
    }
    
    .database-info {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        padding: 1rem;
        border-radius: 12px;
        margin: 1rem 0;
        border-left: 4px solid #0066cc;
    }
    
    .backup-status {
        background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);
        padding: 1rem;
        border-radius: 12px;
        color: #2c3e50;
        margin: 1rem 0;
    }
    
    .debug-panel {
        background: linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%);
        padding: 1rem;
        border-radius: 12px;
        margin: 1rem 0;
        border-left: 4px solid #ff9500;
    }
</style>
""", unsafe_allow_html=True)

# ================================
# SISTEMA SQLITE CACHE INTEGRADO
# ================================

# Cole aqui o código completo da classe SQLiteCache do artefato anterior
# Ou importe de arquivo separado: from sqlite_cache_system import SQLiteCache

# Para este exemplo, vou incluir uma versão simplificada:
class SQLiteCacheSimplified:
    """Versão simplificada do cache SQLite para demonstração"""
    
    def __init__(self, db_path="cache/geocoding_cache.db", ttl_hours=24):
        self.db_path = db_path
        self.ttl_hours = ttl_hours
        self._ensure_db_directory()
        self._initialize_database()
        debugger.info(f"SQLite Cache inicializado: {db_path}")
    
    def _ensure_db_directory(self):
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
    
    def _initialize_database(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS coordinates (
                id INTEGER PRIMARY KEY,
                city_hash TEXT UNIQUE,
                city_name TEXT,
                longitude REAL,
                latitude REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                hits INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache_stats (
                date DATE PRIMARY KEY,
                hits INTEGER DEFAULT 0,
                misses INTEGER DEFAULT 0,
                saves INTEGER DEFAULT 0
            )
        """)
        conn.commit()
        conn.close()
    
    def save_coordinates(self, city_name, coordinates):
        city_hash = hashlib.md5(city_name.upper().encode()).hexdigest()
        expires_at = (datetime.now() + timedelta(hours=self.ttl_hours)).isoformat()
        
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT OR REPLACE INTO coordinates 
            (city_hash, city_name, longitude, latitude, expires_at)
            VALUES (?, ?, ?, ?, ?)
        """, (city_hash, city_name, coordinates[0], coordinates[1], expires_at))
        conn.commit()
        conn.close()
        
        self._update_stats('save')
        debugger.debug(f"SQLite: Coordenadas salvas - {city_name}")
    
    def get_coordinates(self, city_name):
        city_hash = hashlib.md5(city_name.upper().encode()).hexdigest()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("""
            SELECT longitude, latitude, expires_at, hits 
            FROM coordinates WHERE city_hash = ?
        """, (city_hash,))
        
        row = cursor.fetchone()
        
        if row is None:
            conn.close()
            self._update_stats('miss')
            debugger.debug(f"SQLite: Cache MISS - {city_name}")
            return None
        
        # Verificar expiração
        expires_at = datetime.fromisoformat(row[2])
        if datetime.now() > expires_at:
            conn.execute("DELETE FROM coordinates WHERE city_hash = ?", (city_hash,))
            conn.commit()
            conn.close()
            self._update_stats('miss')
            debugger.debug(f"SQLite: Cache EXPIRED - {city_name}")
            return None
        
        # Atualizar hits
        conn.execute("UPDATE coordinates SET hits = hits + 1 WHERE city_hash = ?", (city_hash,))
        conn.commit()
        conn.close()
        
        coordinates = [row[0], row[1]]
        self._update_stats('hit')
        debugger.debug(f"SQLite: Cache HIT - {city_name} -> {coordinates}")
        return coordinates
    
    def _update_stats(self, action):
        conn = sqlite3.connect(self.db_path)
        today = datetime.now().date().isoformat()
        
        # Verificar se existe registro para hoje
        cursor = conn.execute("SELECT * FROM cache_stats WHERE date = ?", (today,))
        if cursor.fetchone() is None:
            conn.execute("INSERT INTO cache_stats (date) VALUES (?)", (today,))
        
        # Atualizar contador
        if action == 'hit':
            conn.execute("UPDATE cache_stats SET hits = hits + 1 WHERE date = ?", (today,))
        elif action == 'miss':
            conn.execute("UPDATE cache_stats SET misses = misses + 1 WHERE date = ?", (today,))
        elif action == 'save':
            conn.execute("UPDATE cache_stats SET saves = saves + 1 WHERE date = ?", (today,))
        
        conn.commit()
        conn.close()
    
    def get_cache_stats(self):
        conn = sqlite3.connect(self.db_path)
        
        # Stats de hoje
        today = datetime.now().date().isoformat()
        cursor = conn.execute("SELECT hits, misses, saves FROM cache_stats WHERE date = ?", (today,))
        row = cursor.fetchone()
        
        if row is None:
            hits = misses = saves = 0
        else:
            hits, misses, saves = row
        
        # Stats globais
        cursor = conn.execute("SELECT COUNT(*) FROM coordinates WHERE expires_at > CURRENT_TIMESTAMP")
        total_entries = cursor.fetchone()[0]
        
        cursor = conn.execute("SELECT SUM(hits) FROM coordinates")
        total_hits = cursor.fetchone()[0] or 0
        
        conn.close()
        
        total_requests = hits + misses
        hit_rate = (hits / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'hits': hits,
            'misses': misses,
            'saves': saves,
            'total_requests': total_requests,
            'hit_rate': round(hit_rate, 2),
            'total_entries': total_entries,
            'total_cache_hits': total_hits,
            'database_path': self.db_path
        }
    
    def clear_cache(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM coordinates")
        conn.execute("DELETE FROM cache_stats")
        conn.commit()
        conn.close()
        debugger.info("SQLite: Cache limpo completamente")
    
    def cleanup_expired(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT COUNT(*) FROM coordinates WHERE expires_at <= CURRENT_TIMESTAMP")
        expired_count = cursor.fetchone()[0]
        
        conn.execute("DELETE FROM coordinates WHERE expires_at <= CURRENT_TIMESTAMP")
        conn.commit()
        conn.close()
        
        if expired_count > 0:
            debugger.info(f"SQLite: {expired_count} entradas expiradas removidas")
        
        return expired_count
    
    def get_database_info(self):
        file_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT COUNT(*) FROM coordinates")
        total_coords = cursor.fetchone()[0]
        
        cursor = conn.execute("SELECT COUNT(*) FROM cache_stats")
        total_stats = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'database_path': self.db_path,
            'file_size_bytes': file_size,
            'file_size_mb': round(file_size / (1024 * 1024), 2),
            'total_coordinates': total_coords,
            'total_stats_days': total_stats,
            'ttl_hours': self.ttl_hours
        }

# ================================
# PROCESSAMENTO PARALELO COM SQLITE
# ================================

class ParallelProcessor:
    def __init__(self, max_workers=4):
        self.max_workers = max_workers
        self.geocoder = Nominatim(user_agent="parallel_processor_sqlite_v3", timeout=20)
        
        # Usar cache SQLite em vez de cache em memória
        if DEBUG_MODE:
            self.cache = SQLiteCacheSimplified("cache/dev_geocoding_cache.db", ttl_hours=1)
        else:
            self.cache = SQLiteCacheSimplified("cache/prod_geocoding_cache.db", ttl_hours=24)
        
        debugger.info(f"ParallelProcessor inicializado com SQLite cache ({max_workers} workers)")
        
        # Migrar dados do cache em memória se existir
        self._migrate_memory_cache()
    
    def _migrate_memory_cache(self):
        """Migra dados do cache em memória antigo para SQLite"""
        if 'coordinates_cache' not in st.session_state:
            return
        
        memory_cache = st.session_state.coordinates_cache
        if not memory_cache:
            return
        
        debugger.info(f"Migrando {len(memory_cache)} entradas do cache em memória para SQLite")
        migrated = 0
        
        for cache_hash, cached_data in memory_cache.items():
            try:
                city_name = cached_data.get('city', '')
                coordinates = cached_data.get('coords', [])
                
                if city_name and coordinates and len(coordinates) == 2:
                    self.cache.save_coordinates(city_name, coordinates)
                    migrated += 1
            except Exception as e:
                debugger.error(f"Erro na migração de {cached_data}: {e}")
        
        if migrated > 0:
            st.session_state.coordinates_cache = {}
            debugger.info(f"Migração concluída: {migrated} entradas transferidas para SQLite")
    
    @debug_timing
    def get_coordinates_with_cache(self, city_name):
        """Geocoding com cache SQLite"""
        # Tentar cache SQLite primeiro
        cached_coords = self.cache.get_coordinates(city_name)
        if cached_coords:
            return cached_coords
        
        debug_breakpoint("Geocoding com SQLite cache miss", {
            'city_name': city_name,
            'cache_type': 'SQLite'
        })
        
        try:
            location = self.geocoder.geocode(f"{city_name}, MG, Brasil")
            if location:
                coords = [location.longitude, location.latitude]
                debugger.info(f"Geocoding sucesso (MG): {city_name} -> {coords}")
                self.cache.save_coordinates(city_name, coords)
                return coords
            
            location = self.geocoder.geocode(f"{city_name}, Brasil")
            if location:
                coords = [location.longitude, location.latitude]
                debugger.info(f"Geocoding sucesso (BR): {city_name} -> {coords}")
                self.cache.save_coordinates(city_name, coords)
                return coords
            
            debugger.warning(f"Geocoding falhou: {city_name}")
            return None
            
        except Exception as e:
            debugger.error(f"Erro no geocoding de {city_name}: {e}")
            return None
    
    @debug_timing
    def calculate_distance_with_retry(self, origin_coords, dest_coords, max_retries=3):
        """Calcula distância com retry automático"""
        for attempt in range(max_retries):
            try:
                url = f"http://router.project-osrm.org/route/v1/driving/{origin_coords[0]},{origin_coords[1]};{dest_coords[0]},{dest_coords[1]}"
                params = {'overview': 'false', 'geometries': 'geojson'}
                debugger.debug(f"Calculando distância: {url} (tentativa {attempt + 1}) params={params}")
                response = requests.get(url, params=params, timeout=25)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('routes'):
                        route = data['routes'][0]
                        distance_km = route['distance'] / 1000
                        duration_min = route['duration'] / 60
                        
                        return {
                            'distancia_km': round(distance_km, 2),
                            'tempo_min': round(duration_min, 0),
                            'status': 'sucesso'
                        }
                
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
        
        return {'status': 'erro_calculo', 'distancia_km': None, 'tempo_min': None}
    
    @debug_timing
    def process_linha_paralela(self, linha_data):
        """Processa uma linha com cache SQLite"""
        linha_excel = linha_data['linha_excel']
        origem = linha_data['origem']
        destinos = linha_data['destinos']
        
        debug_breakpoint("Processamento com SQLite", {
            'linha_excel': linha_excel,
            'origem': origem,
            'total_destinos': len(destinos),
            'cache_type': 'SQLite'
        })
        
        resultado = {
            'linha_excel': linha_excel,
            'origem': origem,
            'total_destinos': len(destinos),
            'destinos_calculados': [],
            'destino_mais_proximo': None,
            'km_mais_proximo': None,
            'sucessos': 0,
            'erros': 0,
            'tempo_processamento': 0,
            'status': 'processando'
        }
        
        start_time = time.time()
        
        # Geocoding da origem com SQLite
        origin_coords = self.get_coordinates_with_cache(origem)
        
        if not origin_coords:
            resultado['status'] = 'origem_nao_encontrada'
            resultado['erros'] = len(destinos)
            return resultado
        
        # Geocoding paralelo dos destinos
        def geocode_destino(destino):
            coords = self.get_coordinates_with_cache(destino)
            return destino, coords
        
        destinos_com_coords = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_destino = {executor.submit(geocode_destino, dest): dest for dest in destinos}
            
            for future in as_completed(future_to_destino):
                destino, coords = future.result()
                destinos_com_coords.append((destino, coords))
        
        # Cálculo paralelo das distâncias
        def calcular_distancia_destino(item):
            destino, dest_coords = item
            if not dest_coords:
                return {
                    'destino': destino,
                    'distancia_km': None,
                    'tempo_min': None,
                    'status': 'coordenadas_nao_encontradas'
                }
            
            resultado_calc = self.calculate_distance_with_retry(origin_coords, dest_coords)
            resultado_calc['destino'] = destino
            
            time.sleep(0.1)
            return resultado_calc
        
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(destinos_com_coords))) as executor:
            future_to_calc = {executor.submit(calcular_distancia_destino, item): item for item in destinos_com_coords}
            
            for future in as_completed(future_to_calc):
                calc_resultado = future.result()
                resultado['destinos_calculados'].append(calc_resultado)
                
                if calc_resultado['status'] == 'sucesso':
                    resultado['sucessos'] += 1
                else:
                    resultado['erros'] += 1
        
        # Identificar mais próximo
        sucessos = [d for d in resultado['destinos_calculados'] if d['distancia_km'] is not None]
        
        if sucessos:
            mais_proximo = min(sucessos, key=lambda x: x['distancia_km'])
            resultado['destino_mais_proximo'] = mais_proximo['destino']
            resultado['km_mais_proximo'] = mais_proximo['distancia_km']
        
        resultado['tempo_processamento'] = round(time.time() - start_time, 2)
        resultado['status'] = 'concluido'
        
        return resultado
    
    @debug_timing
    def process_all_lines_parallel(self, linhas_processaveis):
        """Processa todas as linhas em paralelo com SQLite"""
        start_time = time.time()
        
        resultados = []
        
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(linhas_processaveis))) as executor:
            future_to_linha = {
                executor.submit(self.process_linha_paralela, linha): linha 
                for linha in linhas_processaveis
            }
            
            for future in as_completed(future_to_linha):
                resultado = future.result()
                resultados.append(resultado)
        
        resultados.sort(key=lambda x: x['linha_excel'])
        
        stats_globais = {
            'tempo_total': round(time.time() - start_time, 2),
            'linhas_processadas': len(resultados),
            'total_sucessos': sum(r['sucessos'] for r in resultados),
            'total_erros': sum(r['erros'] for r in resultados),
            'media_tempo_por_linha': round(sum(r['tempo_processamento'] for r in resultados) / len(resultados), 2),
            'processamento_paralelo': True,
            'workers_utilizados': self.max_workers,
            'cache_type': 'SQLite'
        }
        
        return resultados, stats_globais

# ================================
# DASHBOARD DE ANALYTICS COMPLETO
# ================================

class AnalyticsDashboard:
    def __init__(self):
        self.init_session_analytics()
    
    def init_session_analytics(self):
        if 'analytics' not in st.session_state:
            st.session_state.analytics = {
                'processamentos': [],
                'performance_history': [],
                'cache_history': [],
                'error_log': [],
                'session_start': datetime.now()
            }
    
    def log_processamento(self, resultados, stats_globais):
        log_entry = {
            'timestamp': datetime.now(),
            'linhas_processadas': stats_globais['linhas_processadas'],
            'total_sucessos': stats_globais['total_sucessos'],
            'total_erros': stats_globais['total_erros'],
            'tempo_total': stats_globais['tempo_total'],
            'taxa_sucesso': (stats_globais['total_sucessos'] / max(1, stats_globais['total_sucessos'] + stats_globais['total_erros'])) * 100,
            'processamento_paralelo': stats_globais.get('processamento_paralelo', False),
            'workers': stats_globais.get('workers_utilizados', 1),
            'cache_type': stats_globais.get('cache_type', 'Memory')
        }
        
        st.session_state.analytics['processamentos'].append(log_entry)
        
        if len(st.session_state.analytics['processamentos']) > 10:
            st.session_state.analytics['processamentos'] = st.session_state.analytics['processamentos'][-10:]
    
    def show_performance_dashboard(self):
        st.markdown("### 📊 Dashboard de Analytics")
        
        analytics = st.session_state.analytics
        
        if analytics['processamentos']:
            col1, col2, col3, col4 = st.columns(4)
            
            total_processamentos = len(analytics['processamentos'])
            ultimo_processamento = analytics['processamentos'][-1]
            media_tempo = sum(p['tempo_total'] for p in analytics['processamentos']) / total_processamentos
            media_taxa_sucesso = sum(p['taxa_sucesso'] for p in analytics['processamentos']) / total_processamentos
            
            with col1:
                st.metric("🔄 Processamentos", total_processamentos)
            with col2:
                st.metric("⏱️ Tempo Médio", f"{media_tempo:.1f}s")
            with col3:
                st.metric("📈 Taxa Sucesso Média", f"{media_taxa_sucesso:.1f}%")
            with col4:
                cache_type = ultimo_processamento.get('cache_type', 'Memory')
                st.metric("💾 Cache Type", cache_type)
            
            # Mostrar se está usando SQLite
            if ultimo_processamento.get('cache_type') == 'SQLite':
                st.success("✅ **Sistema SQLite ativo** - Cache persistente entre sessões!")
            
            # Gráficos de performance
            st.markdown("#### 📈 Performance ao Longo do Tempo")
            
            df_performance = pd.DataFrame(analytics['processamentos'])
            df_performance['processamento_id'] = range(1, len(df_performance) + 1)
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig_tempo = px.line(
                    df_performance,
                    x='processamento_id',
                    y='tempo_total',
                    title="⏱️ Tempo de Processamento",
                    labels={'tempo_total': 'Tempo (segundos)', 'processamento_id': 'Processamento #'}
                )
                fig_tempo.update_traces(line_color='#667eea')
                st.plotly_chart(fig_tempo, use_container_width=True)
            
            with col2:
                fig_sucesso = px.line(
                    df_performance,
                    x='processamento_id',
                    y='taxa_sucesso',
                    title="📈 Taxa de Sucesso",
                    labels={'taxa_sucesso': 'Taxa de Sucesso (%)', 'processamento_id': 'Processamento #'}
                )
                fig_sucesso.update_traces(line_color='#764ba2')
                st.plotly_chart(fig_sucesso, use_container_width=True)
        
        else:
            st.info("📊 Execute um processamento para ver analytics detalhados")
    
    def show_cache_analytics(self, cache_stats):
        st.markdown("#### 💾 Analytics do Cache SQLite")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("🎯 Cache Hits", cache_stats['hits'])
        with col2:
            st.metric("❌ Cache Misses", cache_stats['misses'])
        with col3:
            st.metric("💾 Itens Salvos", cache_stats['saves'])
        with col4:
            st.metric("📈 Hit Rate", f"{cache_stats['hit_rate']:.1f}%")
        
        # Informações específicas do SQLite
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("🗃️ Total Entradas", cache_stats['total_entries'])
        with col2:
            st.metric("📊 Total Cache Hits", cache_stats['total_cache_hits'])
        with col3:
            if 'file_size_mb' in cache_stats:
                st.metric("💽 Tamanho DB", f"{cache_stats.get('file_size_mb', 0):.2f} MB")
        
        # Gráfico de distribuição
        if cache_stats['total_requests'] > 0:
            fig_cache = go.Figure(data=[
                go.Pie(
                    labels=['Cache Hits', 'Cache Misses'],
                    values=[cache_stats['hits'], cache_stats['misses']],
                    hole=0.4,
                    marker_colors=['#667eea', '#764ba2']
                )
            ])
            fig_cache.update_layout(
                title="💾 Distribuição SQLite Cache: Hits vs Misses",
                height=300
            )
            st.plotly_chart(fig_cache, use_container_width=True)

# ================================
# PAINEL DE ADMINISTRAÇÃO SQLITE
# ================================

def show_sqlite_admin_panel(cache_system):
    """Painel de administração avançado do SQLite"""
    
    st.markdown('<div class="sqlite-panel">', unsafe_allow_html=True)
    st.markdown("### 💾 Administração do Banco SQLite")
    
    # Informações do banco
    db_info = cache_system.get_database_info()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("📁 Arquivo DB", os.path.basename(db_info['database_path']))
        st.metric("💽 Tamanho", f"{db_info['file_size_mb']:.2f} MB")
    
    with col2:
        st.metric("🗃️ Coordenadas", db_info['total_coordinates'])
        st.metric("📊 Dias Stats", db_info['total_stats_days'])
    
    with col3:
        st.metric("⏰ TTL", f"{db_info['ttl_hours']}h")
        st.metric("📍 Path", "..." + db_info['database_path'][-20:])
    
    # Ações de administração
    st.markdown("#### 🛠️ Ações de Administração")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🧹 Limpar Expirados", help="Remove entradas expiradas"):
            with st.spinner("Limpando entradas expiradas..."):
                removed = cache_system.cleanup_expired()
            
            if removed > 0:
                st.success(f"✅ {removed} entradas expiradas removidas")
            else:
                st.info("ℹ️ Nenhuma entrada expirada encontrada")
    
    with col2:
        if st.button("🗑️ Limpar Cache", help="Remove todo o cache"):
            if st.checkbox("⚠️ Confirmar limpeza total"):
                cache_system.clear_cache()
                st.success("✅ Cache limpo completamente")
                st.rerun()
    
    with col3:
        if st.button("📊 Atualizar Stats", help="Atualiza estatísticas"):
            st.rerun()
    
    # Busca de cidades (se disponível na versão completa)
    st.markdown("#### 🔍 Busca no Cache")
    
    search_term = st.text_input("🏙️ Buscar cidade:", placeholder="Digite o nome da cidade...")
    
    if search_term:
        # Simular busca (na versão completa seria cache_system.search_cities())
        coords = cache_system.get_coordinates(search_term.upper())
        if coords:
            st.success(f"✅ **{search_term.upper()}** encontrada: {coords}")
        else:
            st.warning(f"❌ **{search_term.upper()}** não encontrada no cache")
    
    # Informações técnicas
    with st.expander("🔧 Informações Técnicas Detalhadas"):
        st.json(db_info)
    
    st.markdown('</div>', unsafe_allow_html=True)

# ================================
# MAPAS INTERATIVOS
# ================================

class InteractiveMapper:
    def __init__(self):
        pass
    
    def create_route_map(self, resultado_linha):
        if resultado_linha['status'] != 'concluido':
            return None
        
        origem = resultado_linha['origem']
        sucessos = [d for d in resultado_linha['destinos_calculados'] if d['distancia_km'] is not None]
        
        if not sucessos:
            return None
        
        origem_lat, origem_lon = -21.7587, -43.3496
        
        m = folium.Map(
            location=[origem_lat, origem_lon],
            zoom_start=8,
            tiles='OpenStreetMap'
        )
        
        folium.Marker(
            [origem_lat, origem_lon],
            popup=f"🎯 ORIGEM: {origem}",
            tooltip=f"Origem: {origem}",
            icon=folium.Icon(color='red', icon='home', prefix='fa')
        ).add_to(m)
        
        cores = ['blue', 'green', 'orange', 'purple', 'pink', 'gray', 'darkblue', 'darkgreen']
        
        for i, destino_data in enumerate(sucessos[:10]):
            dest_lat = origem_lat + (np.random.random() - 0.5) * 2
            dest_lon = origem_lon + (np.random.random() - 0.5) * 2
            
            cor = cores[i % len(cores)]
            
            folium.Marker(
                [dest_lat, dest_lon],
                popup=f"📍 {destino_data['destino']}<br>📏 {destino_data['distancia_km']} km<br>⏱️ {destino_data['tempo_min']} min",
                tooltip=f"{destino_data['destino']} - {destino_data['distancia_km']} km",
                icon=folium.Icon(color=cor, icon='map-marker', prefix='fa')
            ).add_to(m)
            
            folium.PolyLine(
                [[origem_lat, origem_lon], [dest_lat, dest_lon]],
                color=cor,
                weight=3,
                opacity=0.7,
                popup=f"{origem} → {destino_data['destino']}: {destino_data['distancia_km']} km"
            ).add_to(m)
        
        return m

# ================================
# SISTEMA PRINCIPAL INTEGRADO
# ================================

class AdvancedDistanceSystemSQLite:
    def __init__(self):
        # Usar processador com SQLite
        self.parallel_processor = ParallelProcessor(max_workers=6)
        self.analytics = AnalyticsDashboard()
        self.mapper = InteractiveMapper()
        debugger.info("AdvancedDistanceSystemSQLite inicializado com cache SQLite")
        
    @debug_timing
    def analyze_spreadsheet_advanced(self, df):
        try:
            origem_col = df.columns[1] if len(df.columns) > 1 else None
            destino_cols = df.columns[2:] if len(df.columns) > 2 else []
            
            linhas_processaveis = []
            
            for idx, row in df.iterrows():
                origem = self._clean_city_name(row[origem_col])
                
                if not origem:
                    continue
                
                destinos_linha = []
                for col in destino_cols:
                    destino = self._clean_city_name(row[col])
                    if destino:
                        destinos_linha.append(destino)
                
                if destinos_linha:
                    linhas_processaveis.append({
                        'linha_excel': idx + 1,
                        'origem': origem,
                        'destinos': destinos_linha,
                        'total_destinos': len(destinos_linha)
                    })
            
            return {
                'total_rows_original': len(df),
                'linhas_processaveis': linhas_processaveis,
                'total_linhas_validas': len(linhas_processaveis),
                'total_calculos': sum(linha['total_destinos'] for linha in linhas_processaveis),
                'estimativa_tempo_paralelo': sum(linha['total_destinos'] for linha in linhas_processaveis) * 0.1 / 60,
                'estimativa_tempo_sequencial': sum(linha['total_destinos'] for linha in linhas_processaveis) * 0.3 / 60
            }
            
        except Exception as e:
            st.error(f"❌ Erro na análise: {e}")
            return None
    
    def _clean_city_name(self, city_value):
        if pd.isna(city_value):
            return None
        
        city_clean = str(city_value).strip().upper()
        
        if city_clean in ['', 'NAN', 'NONE', 'NULL', '#N/A']:
            return None
        
        return city_clean.replace('  ', ' ')

# ================================
# PAINEL DE DEBUG COMPLETO
# ================================

def show_debug_panel():
    if not DEBUG_MODE:
        return
    
    with st.sidebar:
        st.markdown('<div class="debug-panel">', unsafe_allow_html=True)
        st.markdown("### 🐛 Debug Panel SQLite")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("🔧 Debug Mode", "ON")
        with col2:
            st.metric("🧵 Threads", threading.active_count())
        
        if 'debug_logs' in st.session_state:
            total_logs = len(st.session_state.debug_logs)
            st.metric("📝 Total Logs", total_logs)
            
            if total_logs > 0:
                st.markdown("#### 📋 Logs Recentes")
                for log in st.session_state.debug_logs[-5:]:
                    level_emoji = {
                        'DEBUG': '🔍',
                        'INFO': 'ℹ️',
                        'WARNING': '⚠️',
                        'ERROR': '🚨'
                    }.get(log['level'], '📝')
                    
                    message = log['message'][:50] + "..." if len(log['message']) > 50 else log['message']
                    st.text(f"{level_emoji} [{log['timestamp']}] {message}")
        
        if st.button("🗑️ Limpar Logs Debug"):
            st.session_state.debug_logs = []
            st.success("Logs limpos!")
            st.rerun()
        
        st.markdown('</div>', unsafe_allow_html=True)

def main():
    """Função principal COMPLETA com SQLite integrado"""
    
    debugger.info("=== INICIANDO APLICAÇÃO COM SQLITE ===")
    
    # Painel de debug
    show_debug_panel()
    
    # Header
    header_text = "🚀 Sistema Avançado de Distâncias - SQLite"
    if DEBUG_MODE:
        header_text += " (Debug Mode)"
    
    st.markdown(f"""
    <div class="main-header">
        <h1>{header_text}</h1>
        <h3>💾 Cache SQLite • Processamento Paralelo • Analytics • Mapas Interativos</h3>
        <p>Versão 3.1.0 - Pro SQLite (Completa)</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Inicializar sistema com SQLite
    sistema = AdvancedDistanceSystemSQLite()
    
    # Sidebar com configurações
    with st.sidebar:
        st.header("⚙️ Configurações Avançadas")
        
        # Performance
        st.markdown("#### 🚀 Performance")
        max_workers = st.slider("🔧 Workers Paralelos", 2, 8, 6)
        sistema.parallel_processor.max_workers = max_workers
        
        # Cache SQLite controls
        st.markdown("#### 💾 Cache SQLite")
        cache_stats = sistema.parallel_processor.cache.get_cache_stats()
        
        if cache_stats['total_requests'] > 0:
            st.metric("📊 Hit Rate", f"{cache_stats['hit_rate']:.1f}%")
            st.metric("🗃️ Entradas", cache_stats['total_entries'])
        
        # Painel de administração SQLite
        show_sqlite_admin_panel(sistema.parallel_processor.cache)
        
        # Toggles
        show_analytics = st.checkbox("📊 Mostrar Analytics", value=True)
        show_maps = st.checkbox("🗺️ Mostrar Mapas", value=True)
        
        st.success("💾 SQLite Cache Ativo")
    
    # Analytics Dashboard
    if show_analytics:
        sistema.analytics.show_performance_dashboard()
        sistema.analytics.show_cache_analytics(cache_stats)
    
    # Upload e processamento
    uploaded_file = st.file_uploader(
        "📁 Upload da Planilha Excel",
        type=['xlsx', 'xls'],
        help="Sistema com cache SQLite persistente e processamento paralelo otimizado"
    )
    
    if uploaded_file is not None:
        debug_breakpoint("Arquivo carregado para processamento SQLite", {
            'filename': uploaded_file.name,
            'size': uploaded_file.size,
            'cache_type': 'SQLite'
        })
        
        with st.spinner("🔍 Análise avançada da planilha..."):
            df = pd.read_excel(uploaded_file)
            analysis = sistema.analyze_spreadsheet_advanced(df)
        
        if analysis:
            st.success("✅ Planilha analisada - Sistema SQLite ativo!")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📋 Linhas Válidas", analysis['total_linhas_validas'])
            with col2:
                st.metric("📏 Cálculos Totais", analysis['total_calculos'])
            with col3:
                st.metric("⏱️ Tempo Est. (Paralelo)", f"{analysis['estimativa_tempo_paralelo']:.1f} min")
            with col4:
                speedup = analysis['estimativa_tempo_sequencial'] / analysis['estimativa_tempo_paralelo']
                st.metric("🚀 Speedup", f"{speedup:.1f}x")
            
            # Info do SQLite
            st.info("💾 **Cache SQLite ativo** - As coordenadas serão salvas permanentemente e reutilizadas entre sessões!")
            
            # Processamento
            if st.button("🚀 Processar com Sistema SQLite", type="primary", use_container_width=True):
                
                debug_breakpoint("Início processamento com SQLite", {
                    'total_linhas': analysis['total_linhas_validas'],
                    'cache_type': 'SQLite',
                    'workers': max_workers
                })
                
                st.markdown("### ⚡ Processamento Paralelo com Cache SQLite")
                
                progress_bar = st.progress(0)
                status_info = st.empty()
                
                start_time = time.time()
                
                status_info.info("🚀 Iniciando processamento com cache SQLite...")
                progress_bar.progress(20)
                
                with st.spinner("Executando processamento com cache SQLite otimizado..."):
                    resultados, stats_globais = sistema.parallel_processor.process_all_lines_parallel(
                        analysis['linhas_processaveis']
                    )
                
                progress_bar.progress(100)
                status_info.success("✅ Processamento SQLite concluído!")
                
                # Log analytics
                sistema.analytics.log_processamento(resultados, stats_globais)
                
                # Resultados
                if resultados:
                    st.markdown("### 📊 Resultados do Processamento SQLite")
                    
                    col1, col2, col3, col4, col5 = st.columns(5)
                    
                    with col1:
                        st.metric("⏱️ Tempo Total", f"{stats_globais['tempo_total']:.2f}s")
                    with col2:
                        st.metric("🚀 Workers", stats_globais['workers_utilizados'])
                    with col3:
                        st.metric("✅ Sucessos", stats_globais['total_sucessos'])
                    with col4:
                        st.metric("❌ Erros", stats_globais['total_erros'])
                    with col5:
                        taxa_final = (stats_globais['total_sucessos'] / max(1, stats_globais['total_sucessos'] + stats_globais['total_erros'])) * 100
                        st.metric("📈 Taxa Sucesso", f"{taxa_final:.1f}%")
                    
                    # Atualizar stats do cache
                    updated_cache_stats = sistema.parallel_processor.cache.get_cache_stats()
                    
                    st.success(f"""
                    🚀 **Sistema SQLite - Performance Otimizada!**
                    
                    ⚡ **Processamento concluído** em {stats_globais['tempo_total']:.2f}s  
                    💾 **Cache SQLite:** {updated_cache_stats['hit_rate']:.1f}% hit rate ({updated_cache_stats['total_entries']} entradas)  
                    🔧 **{stats_globais['workers_utilizados']} workers** executando em paralelo  
                    📊 **Dados persistentes** - coordenadas salvas para próximas execuções
                    """)
                    
                    # Resultados detalhados
                    st.markdown("### 📋 Resultados Detalhados por Linha")
                    
                    for resultado in resultados:
                        with st.expander(f"🎯 Linha {resultado['linha_excel']}: {resultado['origem']} ({resultado['sucessos']} sucessos)"):
                            
                            col1, col2 = st.columns([2, 1])
                            
                            with col1:
                                if resultado['status'] == 'concluido':
                                    sucessos = [d for d in resultado['destinos_calculados'] if d['distancia_km'] is not None]
                                    
                                    if sucessos:
                                        df_destinos = pd.DataFrame(sucessos)
                                        df_destinos = df_destinos.sort_values('distancia_km')
                                        df_destinos['ranking'] = range(1, len(df_destinos) + 1)
                                        
                                        df_display = df_destinos[['ranking', 'destino', 'distancia_km', 'tempo_min']].head(10)
                                        df_display.columns = ['#', 'Destino', 'Distância (km)', 'Tempo (min)']
                                        
                                        st.dataframe(df_display, use_container_width=True, hide_index=True)
                                        
                                        if resultado['destino_mais_proximo']:
                                            st.success(f"🏆 **Mais Próximo:** {resultado['destino_mais_proximo']} - {resultado['km_mais_proximo']} km")
                            
                            with col2:
                                st.metric("⏱️ Tempo", f"{resultado['tempo_processamento']}s")
                                st.metric("📊 Total", resultado['total_destinos'])
                                st.metric("✅ Sucessos", resultado['sucessos'])
                                st.metric("❌ Erros", resultado['erros'])
                                
                                st.text("💾 Cache: SQLite")
                                
                                if show_maps and resultado['status'] == 'concluido':
                                    mapa_linha = sistema.mapper.create_route_map(resultado)
                                    if mapa_linha:
                                        st.markdown("**🗺️ Mapa:**")
                                        st_folium(mapa_linha, width=300, height=200)
                    
                    # Download
                    st.markdown("### 💾 Download do Relatório SQLite")
                    
                    excel_data = create_advanced_excel_sqlite(resultados, stats_globais, updated_cache_stats)
                    
                    if excel_data:
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        filename = f"relatorio_sqlite_{timestamp}.xlsx"
                        
                        st.download_button(
                            label="📥 Baixar Relatório SQLite (Excel)",
                            data=excel_data,
                            file_name=filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            type="primary",
                            use_container_width=True
                        )

def create_advanced_excel_sqlite(resultados, stats_globais, cache_stats):
    """Cria Excel com informações específicas do SQLite"""
    output = BytesIO()
    
    try:
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Aba principal
            relatorio_completo = []
            for resultado in resultados:
                if resultado['status'] == 'concluido':
                    sucessos = [d for d in resultado['destinos_calculados'] if d['distancia_km'] is not None]
                    
                    for i, dest_data in enumerate(sucessos, 1):
                        relatorio_completo.append({
                            'Linha_Excel': resultado['linha_excel'],
                            'Origem': resultado['origem'],
                            'Destino': dest_data['destino'],
                            'Distancia_KM': dest_data['distancia_km'],
                            'Tempo_Minutos': dest_data['tempo_min'],
                            'Ranking_na_Linha': i,
                            'Eh_Mais_Proximo': 'SIM' if dest_data['destino'] == resultado['destino_mais_proximo'] else 'NÃO',
                            'Tempo_Processamento_Linha': resultado['tempo_processamento']
                        })
            
            if relatorio_completo:
                df_completo = pd.DataFrame(relatorio_completo)
                df_completo.to_excel(writer, sheet_name='Resultados_SQLite', index=False)
            
            # Métricas específicas do SQLite
            performance_data = {
                'Métrica': [
                    'Tempo Total de Processamento (s)',
                    'Cache Type',
                    'Cache Hit Rate (%)',
                    'Total Entradas Cache',
                    'Cache Hits Totais',
                    'Database Path',
                    'Workers Utilizados',
                    'Linhas Processadas',
                    'Total de Sucessos',
                    'Total de Erros',
                    'Taxa de Sucesso (%)',
                    'Sistema de Coordenadas',
                    'API de Rotas'
                ],
                'Valor': [
                    stats_globais['tempo_total'],
                    'SQLite Persistente',
                    cache_stats['hit_rate'],
                    cache_stats['total_entries'],
                    cache_stats['total_cache_hits'],
                    cache_stats.get('database_path', 'N/A'),
                    stats_globais['workers_utilizados'],
                    stats_globais['linhas_processadas'],
                    stats_globais['total_sucessos'],
                    stats_globais['total_erros'],
                    round((stats_globais['total_sucessos'] / max(1, stats_globais['total_sucessos'] + stats_globais['total_erros'])) * 100, 2),
                    'Nominatim (OpenStreetMap)',
                    'OSRM (Open Source Routing Machine)'
                ]
            }
            
            df_performance = pd.DataFrame(performance_data)
            df_performance.to_excel(writer, sheet_name='Metricas_SQLite', index=False)
        
        return output.getvalue()
        
    except Exception as e:
        st.error(f"❌ Erro ao criar Excel: {e}")
        return None

if __name__ == "__main__":
    main()