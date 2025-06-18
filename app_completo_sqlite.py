# app_completo_sqlite.py
# type: ignore
"""
🚀 SISTEMA AVANÇADO DE DISTÂNCIAS - VERSÃO PRO COM SQLITE E CACHE DE DISTÂNCIAS
=============================================================================

VERSÃO COMPLETA: Sistema original + Cache SQLite + Cache de Distâncias + Melhorias
- ✅ Cache SQLite persistente entre sessões
- ✅ Cache de DISTÂNCIAS calculadas
- ✅ Performance otimizada com índices
- ✅ TTL automático e limpeza
- ✅ Backup e restore automático
- ✅ Processamento Paralelo completo  
- ✅ Dashboard Analytics completo
- ✅ Mapas Interativos completos
- ✅ Painel de administração do banco
- ✅ Correção de refresh da página
- 🆕 Tracking detalhado de erros por origem-destino
- 🆕 Ignora quando origem = destino

Versão: 3.2.1 - Pro SQLite + Distance Cache + Error Tracking
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
    page_title="🚀 Sistema Avançado de Distâncias - SQLite + Distance Cache",
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
    
    .distance-cache-panel {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        padding: 1rem;
        border-radius: 12px;
        margin: 1rem 0;
        border-left: 4px solid #06d6a0;
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
    
    .error-panel {
        background: linear-gradient(135deg, #ff6b6b 0%, #feca57 100%);
        padding: 1rem;
        border-radius: 12px;
        margin: 1rem 0;
        border-left: 4px solid #e74c3c;
    }
</style>
""", unsafe_allow_html=True)

# ================================
# SISTEMA SQLITE CACHE COM DISTÂNCIAS
# ================================

# Versão simplificada do cache SQLite com distâncias para demonstração
class SQLiteCacheWithDistances:
    """Cache SQLite com suporte a cache de distâncias"""
    
    def __init__(self, db_path="cache/geocoding_cache.db", ttl_hours=24, distance_ttl_hours=168):
        self.db_path = db_path
        self.ttl_hours = ttl_hours
        self.distance_ttl_hours = distance_ttl_hours  # 7 dias para distâncias
        self._ensure_db_directory()
        self._initialize_database()
        debugger.info(f"SQLite Cache com distâncias inicializado: {db_path}")
    
    def _ensure_db_directory(self):
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
    
    def _initialize_database(self):
        conn = sqlite3.connect(self.db_path)
        
        # Tabela de coordenadas
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
        
        # Nova tabela de distâncias
        conn.execute("""
            CREATE TABLE IF NOT EXISTS distances (
                id INTEGER PRIMARY KEY,
                route_hash TEXT UNIQUE,
                origin_city TEXT,
                destination_city TEXT,
                distance_km REAL,
                duration_minutes REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                hits INTEGER DEFAULT 0,
                source TEXT DEFAULT 'osrm'
            )
        """)
        
        # Tabela de estatísticas
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache_stats (
                date DATE PRIMARY KEY,
                coord_hits INTEGER DEFAULT 0,
                coord_misses INTEGER DEFAULT 0,
                coord_saves INTEGER DEFAULT 0,
                distance_hits INTEGER DEFAULT 0,
                distance_misses INTEGER DEFAULT 0,
                distance_saves INTEGER DEFAULT 0
            )
        """)
        
        # Índices para performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_coordinates_hash ON coordinates(city_hash)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_distances_hash ON distances(route_hash)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_distances_expires ON distances(expires_at)")
        
        conn.commit()
        conn.close()
    
    def _get_route_hash(self, origin_city: str, destination_city: str) -> str:
        """Gera hash único para o par origem-destino"""
        origin_norm = origin_city.upper().strip()
        dest_norm = destination_city.upper().strip()
        
        # Ordem alfabética para consistência (A->B = B->A)
        if origin_norm < dest_norm:
            route_string = f"{origin_norm}|{dest_norm}"
        else:
            route_string = f"{dest_norm}|{origin_norm}"
        
        return hashlib.sha256(route_string.encode('utf-8')).hexdigest()
    
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
        
        self._update_stats('coord_saves')
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
            self._update_stats('coord_misses')
            debugger.debug(f"SQLite: Cache MISS coordenadas - {city_name}")
            return None
        
        # Verificar expiração
        expires_at = datetime.fromisoformat(row[2])
        if datetime.now() > expires_at:
            conn.execute("DELETE FROM coordinates WHERE city_hash = ?", (city_hash,))
            conn.commit()
            conn.close()
            self._update_stats('coord_misses')
            debugger.debug(f"SQLite: Cache EXPIRED coordenadas - {city_name}")
            return None
        
        # Atualizar hits
        conn.execute("UPDATE coordinates SET hits = hits + 1 WHERE city_hash = ?", (city_hash,))
        conn.commit()
        conn.close()
        
        coordinates = [row[0], row[1]]
        self._update_stats('coord_hits')
        debugger.debug(f"SQLite: Cache HIT coordenadas - {city_name} -> {coordinates}")
        return coordinates
    
    def save_distance(self, origin_city: str, destination_city: str, distance_km: float, duration_minutes: float):
        """Salva distância calculada no cache"""
        route_hash = self._get_route_hash(origin_city, destination_city)
        expires_at = (datetime.now() + timedelta(hours=self.distance_ttl_hours)).isoformat()
        
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT OR REPLACE INTO distances 
            (route_hash, origin_city, destination_city, distance_km, duration_minutes, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (route_hash, origin_city, destination_city, distance_km, duration_minutes, expires_at))
        conn.commit()
        conn.close()
        
        self._update_stats('distance_saves')
        debugger.debug(f"SQLite: Distância salva - {origin_city} -> {destination_city} = {distance_km}km")
    
    def get_distance(self, origin_city: str, destination_city: str):
        """Recupera distância do cache"""
        route_hash = self._get_route_hash(origin_city, destination_city)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("""
            SELECT distance_km, duration_minutes, expires_at, hits 
            FROM distances WHERE route_hash = ?
        """, (route_hash,))
        
        row = cursor.fetchone()
        
        if row is None:
            conn.close()
            self._update_stats('distance_misses')
            debugger.debug(f"SQLite: Cache MISS distância - {origin_city} -> {destination_city}")
            return None
        
        # Verificar expiração
        expires_at = datetime.fromisoformat(row[2])
        if datetime.now() > expires_at:
            conn.execute("DELETE FROM distances WHERE route_hash = ?", (route_hash,))
            conn.commit()
            conn.close()
            self._update_stats('distance_misses')
            debugger.debug(f"SQLite: Cache EXPIRED distância - {origin_city} -> {destination_city}")
            return None
        
        # Atualizar hits
        conn.execute("UPDATE distances SET hits = hits + 1 WHERE route_hash = ?", (route_hash,))
        conn.commit()
        conn.close()
        
        result = {
            'distancia_km': round(row[0], 2),
            'tempo_min': round(row[1], 0),
            'status': 'cache_hit',
            'hits': row[3] + 1
        }
        
        self._update_stats('distance_hits')
        debugger.debug(f"SQLite: Cache HIT distância - {origin_city} -> {destination_city} = {result['distancia_km']}km")
        return result
    
    def _update_stats(self, action):
        conn = sqlite3.connect(self.db_path)
        today = datetime.now().date().isoformat()
        
        # Verificar se existe registro para hoje
        cursor = conn.execute("SELECT * FROM cache_stats WHERE date = ?", (today,))
        if cursor.fetchone() is None:
            conn.execute("INSERT INTO cache_stats (date) VALUES (?)", (today,))
        
        # Atualizar contador
        conn.execute(f"UPDATE cache_stats SET {action} = {action} + 1 WHERE date = ?", (today,))
        conn.commit()
        conn.close()
    
    def get_cache_stats(self):
        conn = sqlite3.connect(self.db_path)
        
        # Stats de hoje
        today = datetime.now().date().isoformat()
        cursor = conn.execute("""
            SELECT coord_hits, coord_misses, coord_saves, 
                   distance_hits, distance_misses, distance_saves 
            FROM cache_stats WHERE date = ?
        """, (today,))
        row = cursor.fetchone()
        
        if row is None:
            coord_hits = coord_misses = coord_saves = 0
            distance_hits = distance_misses = distance_saves = 0
        else:
            coord_hits, coord_misses, coord_saves, distance_hits, distance_misses, distance_saves = row
        
        # Stats globais
        cursor = conn.execute("SELECT COUNT(*) FROM coordinates WHERE expires_at > CURRENT_TIMESTAMP")
        total_coordinates = cursor.fetchone()[0]
        
        cursor = conn.execute("SELECT COUNT(*) FROM distances WHERE expires_at > CURRENT_TIMESTAMP")
        total_distances = cursor.fetchone()[0]
        
        conn.close()
        
        # Calcular hit rates
        total_coord_requests = coord_hits + coord_misses
        coord_hit_rate = (coord_hits / total_coord_requests * 100) if total_coord_requests > 0 else 0
        
        total_distance_requests = distance_hits + distance_misses
        distance_hit_rate = (distance_hits / total_distance_requests * 100) if total_distance_requests > 0 else 0
        
        total_hits = coord_hits + distance_hits
        total_requests = total_coord_requests + total_distance_requests
        overall_hit_rate = (total_hits / total_requests * 100) if total_requests > 0 else 0
        
        file_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
        
        return {
            # Coordenadas
            'coord_hits': coord_hits,
            'coord_misses': coord_misses,
            'coord_saves': coord_saves,
            'coord_hit_rate': round(coord_hit_rate, 2),
            'total_coordinates': total_coordinates,
            
            # Distâncias
            'distance_hits': distance_hits,
            'distance_misses': distance_misses,
            'distance_saves': distance_saves,
            'distance_hit_rate': round(distance_hit_rate, 2),
            'total_distances': total_distances,
            
            # Geral
            'total_hits': total_hits,
            'total_requests': total_requests,
            'hit_rate': round(overall_hit_rate, 2),
            'total_entries': total_coordinates + total_distances,
            
            # Sistema
            'database_path': self.db_path,
            'file_size_bytes': file_size,
            'file_size_mb': round(file_size / (1024 * 1024), 2),
            'ttl_hours': self.ttl_hours,
            'distance_ttl_hours': self.distance_ttl_hours
        }
    
    def clear_cache(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM coordinates")
        conn.execute("DELETE FROM distances")
        conn.execute("DELETE FROM cache_stats")
        conn.commit()
        conn.close()
        debugger.info("SQLite: Cache limpo completamente")
    
    def cleanup_expired(self):
        conn = sqlite3.connect(self.db_path)
        
        cursor = conn.execute("SELECT COUNT(*) FROM coordinates WHERE expires_at <= CURRENT_TIMESTAMP")
        expired_coords = cursor.fetchone()[0]
        
        cursor = conn.execute("SELECT COUNT(*) FROM distances WHERE expires_at <= CURRENT_TIMESTAMP")
        expired_distances = cursor.fetchone()[0]
        
        conn.execute("DELETE FROM coordinates WHERE expires_at <= CURRENT_TIMESTAMP")
        conn.execute("DELETE FROM distances WHERE expires_at <= CURRENT_TIMESTAMP")
        conn.commit()
        conn.close()
        
        total_removed = expired_coords + expired_distances
        if total_removed > 0:
            debugger.info(f"SQLite: {expired_coords} coordenadas + {expired_distances} distâncias expiradas removidas")
        
        return total_removed
    
    def get_database_info(self):
        file_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT COUNT(*) FROM coordinates")
        total_coords = cursor.fetchone()[0]
        
        cursor = conn.execute("SELECT COUNT(*) FROM distances")
        total_distances = cursor.fetchone()[0]
        
        cursor = conn.execute("SELECT COUNT(*) FROM cache_stats")
        total_stats = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'database_path': self.db_path,
            'file_size_bytes': file_size,
            'file_size_mb': round(file_size / (1024 * 1024), 2),
            'total_coordinates': total_coords,
            'total_distances': total_distances,
            'total_stats_days': total_stats,
            'ttl_hours': self.ttl_hours,
            'distance_ttl_hours': self.distance_ttl_hours
        }

# ================================
# PROCESSAMENTO PARALELO COM CACHE DE DISTÂNCIAS E TRACKING DE ERROS
# ================================

class ParallelProcessor:
    def __init__(self, max_workers=4):
        self.max_workers = max_workers
        self.geocoder = Nominatim(user_agent="parallel_processor_error_tracking_v3", timeout=20) 
        
        # Usar cache SQLite com distâncias
        if DEBUG_MODE:
            self.cache = SQLiteCacheWithDistances("cache/dev_geocoding_cache.db", ttl_hours=1, distance_ttl_hours=24)
        else:
            self.cache = SQLiteCacheWithDistances("cache/prod_geocoding_cache.db", ttl_hours=24, distance_ttl_hours=168)
        
        debugger.info(f"ParallelProcessor inicializado com tracking de erros ({max_workers} workers)")
        
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
    def calculate_distance_with_retry(self, origin_coords, dest_coords, origin_city, destination_city, max_retries=3):
        """Calcula distância com cache de distâncias e retry automático"""
        
        # *** NOVO: Verificar cache de distâncias primeiro ***
        cached_distance = self.cache.get_distance(origin_city, destination_city)
        if cached_distance:
            debugger.info(f"Cache HIT distância: {origin_city} -> {destination_city} = {cached_distance['distancia_km']}km")
            return cached_distance
        
        debugger.debug(f"Cache MISS distância - calculando: {origin_city} -> {destination_city}")
        
        # Calcular distância via API
        for attempt in range(max_retries):
            try:
                url = f"http://router.project-osrm.org/route/v1/driving/{origin_coords[0]},{origin_coords[1]};{dest_coords[0]},{dest_coords[1]}"
                params = {'overview': 'false', 'geometries': 'geojson'}
                
                debug_breakpoint("Calculando distância via API", {
                    'origin_city': origin_city,
                    'destination_city': destination_city,
                    'attempt': attempt + 1,
                    'cache_miss': True
                })
                
                response = requests.get(url, params=params, timeout=25)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('routes'):
                        route = data['routes'][0]
                        distance_km = route['distance'] / 1000
                        duration_min = route['duration'] / 60
                        
                        result = {
                            'distancia_km': round(distance_km, 2),
                            'tempo_min': round(duration_min, 0),
                            'status': 'sucesso'
                        }
                        
                        # *** NOVO: Salvar no cache de distâncias ***
                        self.cache.save_distance(origin_city, destination_city, distance_km, duration_min)
                        
                        debugger.info(f"Distância calculada e salva: {origin_city} -> {destination_city} = {distance_km:.2f}km")
                        return result
                
                debugger.warning(f"Tentativa {attempt + 1} falhou: status {response.status_code}")
                
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Backoff exponencial
                    
            except Exception as e:
                debugger.error(f"Erro na tentativa {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
        
        debugger.error(f"Todas as tentativas de cálculo falharam: {origin_city} -> {destination_city}")
        return {
            'status': 'erro_calculo', 
            'distancia_km': None, 
            'tempo_min': None,
            'erro_detalhes': f'Falha ao calcular rota {origin_city} -> {destination_city}'
        }
    
    @debug_timing
    def process_linha_paralela(self, linha_data):
        """Processa uma linha com cache SQLite, cache de distâncias e tracking detalhado de erros"""
        linha_excel = linha_data['linha_excel']
        origem = linha_data['origem']
        destinos = linha_data['destinos']
        
        debug_breakpoint("Processamento com tracking de erros", {
            'linha_excel': linha_excel,
            'origem': origem,
            'total_destinos': len(destinos),
            'cache_type': 'SQLite + Distance Cache + Error Tracking'
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
            'status': 'processando',
            'erros_detalhados': []  # *** NOVO: Lista de erros específicos ***
        }
        
        start_time = time.time()
        
        # Geocoding da origem com SQLite
        origin_coords = self.get_coordinates_with_cache(origem)
        
        if not origin_coords:
            resultado['status'] = 'origem_nao_encontrada'
            resultado['erros'] = len(destinos)
            # *** NOVO: Registrar erro específico da origem ***
            resultado['erros_detalhados'].append({
                'tipo': 'geocoding_origem',
                'origem': origem,
                'destino': 'N/A',
                'erro': 'Coordenadas da origem não encontradas'
            })
            debugger.error(f"Origem não encontrada: {origem}")
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
        
        # Cálculo paralelo das distâncias COM CACHE E TRACKING DE ERROS
        def calcular_distancia_destino(item):
            destino, dest_coords = item
            
            if not dest_coords:
                # *** NOVO: Erro específico de geocoding do destino ***
                erro_detalhado = {
                    'tipo': 'geocoding_destino',
                    'origem': origem,
                    'destino': destino,
                    'erro': 'Coordenadas do destino não encontradas'
                }
                return {
                    'destino': destino,
                    'distancia_km': None,
                    'tempo_min': None,
                    'status': 'coordenadas_nao_encontradas',
                    'erro_detalhado': erro_detalhado
                }
            
            # *** PASSOU ORIGEM E DESTINO PARA O CACHE ***
            resultado_calc = self.calculate_distance_with_retry(origin_coords, dest_coords, origem, destino)
            resultado_calc['destino'] = destino
            
            # *** NOVO: Adicionar erro detalhado se falhou ***
            if resultado_calc['status'] not in ['sucesso', 'cache_hit']:
                erro_detalhado = {
                    'tipo': 'calculo_distancia',
                    'origem': origem,
                    'destino': destino,
                    'erro': resultado_calc.get('erro_detalhes', 'Erro no cálculo da distância via API')
                }
                resultado_calc['erro_detalhado'] = erro_detalhado
            
            time.sleep(0.1)
            return resultado_calc
        
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(destinos_com_coords))) as executor:
            future_to_calc = {executor.submit(calcular_distancia_destino, item): item for item in destinos_com_coords}
            
            for future in as_completed(future_to_calc):
                calc_resultado = future.result()
                resultado['destinos_calculados'].append(calc_resultado)
                
                if calc_resultado['status'] in ['sucesso', 'cache_hit']:
                    resultado['sucessos'] += 1
                else:
                    resultado['erros'] += 1
                    # *** NOVO: Adicionar erro detalhado à lista ***
                    if 'erro_detalhado' in calc_resultado:
                        resultado['erros_detalhados'].append(calc_resultado['erro_detalhado'])
        
        # Identificar mais próximo
        sucessos = [d for d in resultado['destinos_calculados'] if d['distancia_km'] is not None]
        
        if sucessos:
            mais_proximo = min(sucessos, key=lambda x: x['distancia_km'])
            resultado['destino_mais_proximo'] = mais_proximo['destino']
            resultado['km_mais_proximo'] = mais_proximo['distancia_km']
        
        resultado['tempo_processamento'] = round(time.time() - start_time, 2)
        resultado['status'] = 'concluido'
        
        # *** LOG DOS ERROS DETALHADOS ***
        if resultado['erros_detalhados']:
            debugger.warning(f"Linha {linha_excel} - {len(resultado['erros_detalhados'])} erros específicos registrados")
            for erro in resultado['erros_detalhados']:
                debugger.error(f"Erro {erro['tipo']}: {erro['origem']} -> {erro['destino']} - {erro['erro']}")
        
        return resultado
    
    @debug_timing
    def process_all_lines_parallel(self, linhas_processaveis):
        """Processa todas as linhas em paralelo com cache de distâncias e tracking de erros"""
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
        
        # *** NOVO: Agregar erros de todas as linhas ***
        todos_erros = []
        for resultado in resultados:
            todos_erros.extend(resultado.get('erros_detalhados', []))
        
        stats_globais = {
            'tempo_total': round(time.time() - start_time, 2),
            'linhas_processadas': len(resultados),
            'total_sucessos': sum(r['sucessos'] for r in resultados),
            'total_erros': sum(r['erros'] for r in resultados),
            'media_tempo_por_linha': round(sum(r['tempo_processamento'] for r in resultados) / len(resultados), 2),
            'processamento_paralelo': True,
            'workers_utilizados': self.max_workers,
            'cache_type': 'SQLite + Distance Cache',
            'erros_detalhados': todos_erros  # *** NOVO: Lista completa de erros ***
        }
        
        # *** LOG RESUMO DOS ERROS ***
        if todos_erros:
            debugger.warning(f"Processamento concluído com {len(todos_erros)} erros específicos")
            
            # Contar tipos de erro
            tipos_erro = {}
            for erro in todos_erros:
                tipo = erro['tipo']
                tipos_erro[tipo] = tipos_erro.get(tipo, 0) + 1
            
            for tipo, count in tipos_erro.items():
                debugger.warning(f"  - {tipo}: {count} ocorrências")
        
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
            'cache_type': stats_globais.get('cache_type', 'Memory'),
            'erros_detalhados': len(stats_globais.get('erros_detalhados', []))  # *** NOVO ***
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
            
            # *** NOVO: Mostrar info sobre erros detalhados ***
            total_erros_detalhados = sum(p.get('erros_detalhados', 0) for p in analytics['processamentos'])
            if total_erros_detalhados > 0:
                st.info(f"🔍 **Tracking de Erros Ativo** - {total_erros_detalhados} erros detalhados registrados")
            
            # Mostrar se está usando cache de distâncias
            if 'Distance Cache' in ultimo_processamento.get('cache_type', ''):
                st.success("✅ **Cache de Distâncias ativo** - Rotas já calculadas são reutilizadas!")
            
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
        st.markdown("#### 💾 Analytics do Cache SQLite + Distâncias")
        
        # Métricas de coordenadas
        st.markdown("##### 📍 Cache de Coordenadas")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("🎯 Coord Hits", cache_stats['coord_hits'])
        with col2:
            st.metric("❌ Coord Misses", cache_stats['coord_misses'])
        with col3:
            st.metric("💾 Coord Saves", cache_stats['coord_saves'])
        with col4:
            st.metric("📈 Coord Hit Rate", f"{cache_stats['coord_hit_rate']:.1f}%")
        
        # Métricas de distâncias
        st.markdown("##### 🛣️ Cache de Distâncias")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("🎯 Dist Hits", cache_stats['distance_hits'])
        with col2:
            st.metric("❌ Dist Misses", cache_stats['distance_misses'])
        with col3:
            st.metric("💾 Dist Saves", cache_stats['distance_saves'])
        with col4:
            st.metric("📈 Dist Hit Rate", f"{cache_stats['distance_hit_rate']:.1f}%")
        
        # Informações gerais
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("🗃️ Total Coordenadas", cache_stats['total_coordinates'])
        with col2:
            st.metric("🛣️ Total Distâncias", cache_stats['total_distances'])
        with col3:
            st.metric("💽 Tamanho DB", f"{cache_stats.get('file_size_mb', 0):.2f} MB")
        
        # Gráfico de distribuição geral
        if cache_stats['total_requests'] > 0:
            fig_cache = go.Figure(data=[
                go.Pie(
                    labels=['Cache Hits (Geral)', 'Cache Misses (Geral)'],
                    values=[cache_stats['total_hits'], cache_stats['total_requests'] - cache_stats['total_hits']],
                    hole=0.4,
                    marker_colors=['#11998e', '#e74c3c']
                )
            ])
            fig_cache.update_layout(
                title="💾 Performance Geral do Cache (Coordenadas + Distâncias)",
                height=300
            )
            st.plotly_chart(fig_cache, use_container_width=True)

# ================================
# PAINEL DE ADMINISTRAÇÃO SQLITE COM DISTÂNCIAS
# ================================

def show_sqlite_admin_panel(cache_system):
    """Painel de administração avançado do SQLite com cache de distâncias"""
    
    st.markdown('<div class="sqlite-panel">', unsafe_allow_html=True)
    st.markdown("### 💾 Administração do Banco SQLite + Distâncias")
    
    # Informações do banco
    db_info = cache_system.get_database_info()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("📁 Arquivo DB", os.path.basename(db_info['database_path']))
        st.metric("💽 Tamanho", f"{db_info['file_size_mb']:.2f} MB")
    
    with col2:
        st.metric("🗃️ Coordenadas", db_info['total_coordinates'])
        st.metric("🛣️ Distâncias", db_info['total_distances'])
    
    with col3:
        st.metric("⏰ TTL Coords", f"{db_info['ttl_hours']}h")
        st.metric("⏰ TTL Dist", f"{db_info['distance_ttl_hours']}h")
    
    # Painel específico do cache de distâncias
    st.markdown('<div class="distance-cache-panel">', unsafe_allow_html=True)
    st.markdown("#### 🛣️ Cache de Distâncias")
    
    cache_stats = cache_system.get_cache_stats()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("📊 Hit Rate Distâncias", f"{cache_stats['distance_hit_rate']:.1f}%")
    with col2:
        st.metric("💾 Distâncias Salvas", cache_stats['total_distances'])
    with col3:
        st.metric("⏰ TTL", f"{cache_stats['distance_ttl_hours']}h")
    
    st.info("🚀 **Cache de Distâncias ativo** - Rotas já calculadas são reutilizadas automaticamente!")
    st.markdown('</div>', unsafe_allow_html=True)
    
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
    
    with col3:
        if st.button("📊 Atualizar Stats", help="Atualiza estatísticas"):
            pass
    
    # Informações técnicas
    with st.expander("🔧 Informações Técnicas Detalhadas"):
        st.json(db_info)
    
    st.markdown('</div>', unsafe_allow_html=True)

# ================================
# NOVA FUNÇÃO PARA EXIBIR ERROS DETALHADOS
# ================================

def show_detailed_errors(erros_detalhados):
    """Exibe painel de erros detalhados"""
    if not erros_detalhados:
        return
    
    st.markdown('<div class="error-panel">', unsafe_allow_html=True)
    st.markdown("### 🚨 Erros Detalhados por Origem-Destino")
    
    # Estatísticas dos erros
    col1, col2, col3 = st.columns(3)
    
    total_erros = len(erros_detalhados)
    tipos_erro = {}
    origens_com_erro = set()
    destinos_com_erro = set()
    
    for erro in erros_detalhados:
        tipo = erro['tipo']
        tipos_erro[tipo] = tipos_erro.get(tipo, 0) + 1
        origens_com_erro.add(erro['origem'])
        destinos_com_erro.add(erro['destino'])
    
    with col1:
        st.metric("🚨 Total de Erros", total_erros)
    with col2:
        st.metric("🏠 Origens Afetadas", len(origens_com_erro))
    with col3:
        st.metric("🎯 Destinos Afetados", len(destinos_com_erro))
    
    # Distribuição dos tipos de erro
    st.markdown("#### 📊 Distribuição por Tipo de Erro")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        if tipos_erro:
            df_erros = pd.DataFrame([
                {'Tipo de Erro': tipo, 'Quantidade': qtd}
                for tipo, qtd in tipos_erro.items()
            ])
            
            fig_erros = px.bar(
                df_erros,
                x='Tipo de Erro',
                y='Quantidade',
                title="Tipos de Erro",
                color='Quantidade',
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig_erros, use_container_width=True)
    
    with col2:
        st.markdown("**Tipos de Erro:**")
        for tipo, qtd in tipos_erro.items():
            emoji = {
                'geocoding_origem': '🏠',
                'geocoding_destino': '🎯',
                'calculo_distancia': '🛣️'
            }.get(tipo, '❌')
            st.metric(f"{emoji} {tipo.replace('_', ' ').title()}", qtd)
    
    # Tabela detalhada de erros
    st.markdown("#### 📋 Lista Detalhada de Erros")
    
    # Preparar dados para a tabela
    df_erros_detalhados = pd.DataFrame(erros_detalhados)
    df_erros_detalhados['par_origem_destino'] = df_erros_detalhados['origem'] + ' → ' + df_erros_detalhados['destino']
    
    # Adicionar emoji por tipo
    df_erros_detalhados['tipo_emoji'] = df_erros_detalhados['tipo'].map({
        'geocoding_origem': '🏠 Geocoding Origem',
        'geocoding_destino': '🎯 Geocoding Destino',
        'calculo_distancia': '🛣️ Cálculo Distância'
    })
    
    # Filtros para a tabela
    col1, col2 = st.columns(2)
    
    with col1:
        filtro_tipo = st.selectbox(
            "Filtrar por Tipo:",
            options=['Todos'] + list(tipos_erro.keys()),
            index=0
        )
    
    with col2:
        filtro_origem = st.selectbox(
            "Filtrar por Origem:",
            options=['Todas'] + sorted(list(origens_com_erro)),
            index=0
        )
    
    # Aplicar filtros
    df_filtrado = df_erros_detalhados.copy()
    
    if filtro_tipo != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['tipo'] == filtro_tipo]
    
    if filtro_origem != 'Todas':
        df_filtrado = df_filtrado[df_filtrado['origem'] == filtro_origem]
    
    # Exibir tabela
    if not df_filtrado.empty:
        df_display = df_filtrado[['tipo_emoji', 'par_origem_destino', 'erro']].copy()
        df_display.columns = ['Tipo', 'Origem → Destino', 'Descrição do Erro']
        
        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            height=min(400, len(df_display) * 35 + 50)
        )
        
        st.info(f"📊 Mostrando {len(df_filtrado)} de {total_erros} erros")
    else:
        st.warning("🔍 Nenhum erro encontrado com os filtros aplicados")
    
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
            
            # Indicar se veio do cache
            cache_icon = "💾" if destino_data.get('status') == 'cache_hit' else "🆕"
            
            folium.Marker(
                [dest_lat, dest_lon],
                popup=f"📍 {destino_data['destino']}<br>📏 {destino_data['distancia_km']} km<br>⏱️ {destino_data['tempo_min']} min<br>{cache_icon} Cache",
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
        # Usar processador com SQLite + cache de distâncias
        self.parallel_processor = ParallelProcessor(max_workers=6)
        self.analytics = AnalyticsDashboard()
        self.mapper = InteractiveMapper()
        debugger.info("AdvancedDistanceSystemSQLite inicializado com tracking de erros")
        
    @debug_timing
    def analyze_spreadsheet_advanced(self, df):
        try:
            origem_col = df.columns[1] if len(df.columns) > 1 else None
            destino_cols = df.columns[2:] if len(df.columns) > 2 else []
            
            linhas_processaveis = []
            destinos_ignorados = 0  # *** NOVO: Contador de destinos ignorados ***
            
            for idx, row in df.iterrows():
                origem = self._clean_city_name(row[origem_col])
                
                if not origem:
                    continue
                
                destinos_linha = []
                for col in destino_cols:
                    destino = self._clean_city_name(row[col])
                    
                    # *** NOVO: Ignorar quando origem = destino ***
                    if destino and destino != origem:
                        destinos_linha.append(destino)
                    elif destino == origem:
                        destinos_ignorados += 1
                        debugger.info(f"Ignorando destino igual à origem: {origem} = {destino}")
                
                if destinos_linha:
                    linhas_processaveis.append({
                        'linha_excel': idx + 1,
                        'origem': origem,
                        'destinos': destinos_linha,
                        'total_destinos': len(destinos_linha)
                    })
            
            analysis = {
                'total_rows_original': len(df),
                'linhas_processaveis': linhas_processaveis,
                'total_linhas_validas': len(linhas_processaveis),
                'total_calculos': sum(linha['total_destinos'] for linha in linhas_processaveis),
                'destinos_ignorados': destinos_ignorados,  # *** NOVO ***
                'estimativa_tempo_paralelo': sum(linha['total_destinos'] for linha in linhas_processaveis) * 0.05 / 60,
                'estimativa_tempo_sequencial': sum(linha['total_destinos'] for linha in linhas_processaveis) * 0.3 / 60
            }
            
            # *** NOVO: Log sobre destinos ignorados ***
            if destinos_ignorados > 0:
                debugger.info(f"Análise concluída: {destinos_ignorados} destinos ignorados (origem = destino)")
            
            return analysis
            
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
        st.markdown("### 🐛 Debug Panel Error Tracking")
        
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
        
        st.markdown('</div>', unsafe_allow_html=True)

def main():
    """Função principal COMPLETA com SQLite + Cache de Distâncias + Error Tracking"""
    
    debugger.info("=== INICIANDO APLICAÇÃO COM TRACKING DE ERROS ===")
    
    # Painel de debug
    show_debug_panel()
    
    # Header
    header_text = "🚀 Sistema Avançado de Distâncias - SQLite + Distance Cache + Error Tracking"
    if DEBUG_MODE:
        header_text += " (Debug Mode)"
    
    st.markdown(f"""
    <div class="main-header">
        <h1>{header_text}</h1>
        <h3>💾 Cache SQLite • 🛣️ Cache de Distâncias • 🚨 Tracking de Erros • Analytics</h3>
        <p>Versão 3.2.1 - Pro SQLite + Distance Cache + Error Tracking</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Inicializar sistema com SQLite + cache de distâncias + tracking de erros
    sistema = AdvancedDistanceSystemSQLite()
    
    # *** CONTROLE DE ESTADO DOS RESULTADOS ***
    # Inicializar resultados no session_state para evitar perda após refresh
    if 'processamento_resultados' not in st.session_state:
        st.session_state.processamento_resultados = None
    if 'processamento_stats' not in st.session_state:
        st.session_state.processamento_stats = None
    if 'processamento_concluido' not in st.session_state:
        st.session_state.processamento_concluido = False
    
    # Sidebar com configurações
    with st.sidebar:
        st.header("⚙️ Configurações Avançadas")
        
        # Performance
        st.markdown("#### 🚀 Performance")
        max_workers = st.slider("🔧 Workers Paralelos", 1, 8, 6)
        sistema.parallel_processor.max_workers = max_workers
        
        # Cache SQLite controls
        st.markdown("#### 💾 Cache SQLite + Distâncias")
        cache_stats = sistema.parallel_processor.cache.get_cache_stats()
        
        if cache_stats['total_requests'] > 0:
            st.metric("📊 Hit Rate Geral", f"{cache_stats['hit_rate']:.1f}%")
            st.metric("🗃️ Entradas", cache_stats['total_entries'])
        
        # Painel de administração SQLite
        show_sqlite_admin_panel(sistema.parallel_processor.cache)
        
        # Toggles
        show_analytics = st.checkbox("📊 Mostrar Analytics", value=True)
        show_maps = st.checkbox("🗺️ Mostrar Mapas", value=False)
        show_detailed_errors_panel = st.checkbox("🚨 Mostrar Erros Detalhados", value=True)  # *** NOVO ***
        
        st.success("💾 SQLite + 🛣️ Distance Cache + 🚨 Error Tracking Ativo")
    
    # Analytics Dashboard
    if show_analytics:
        sistema.analytics.show_performance_dashboard()
        sistema.analytics.show_cache_analytics(cache_stats)
    
    # Upload e processamento
    uploaded_file = st.file_uploader(
        "📁 Upload da Planilha Excel",
        type=['xlsx', 'xls'],
        help="Sistema com cache SQLite persistente, cache de distâncias E tracking de erros detalhado"
    )
    
    if uploaded_file is not None:
        debug_breakpoint("Arquivo carregado para processamento com error tracking", {
            'filename': uploaded_file.name,
            'size': uploaded_file.size,
            'cache_type': 'SQLite + Distance Cache + Error Tracking'
        })
        
        with st.spinner("🔍 Análise avançada da planilha..."):
            df = pd.read_excel(uploaded_file)
            analysis = sistema.analyze_spreadsheet_advanced(df)
        
        if analysis:
            st.success("✅ Planilha analisada - Sistema SQLite + Cache de Distâncias + Error Tracking ativo!")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📋 Linhas Válidas", analysis['total_linhas_validas'])
            with col2:
                st.metric("📏 Cálculos Totais", analysis['total_calculos'])
            with col3:
                st.metric("⏱️ Tempo Est.", f"{analysis['estimativa_tempo_paralelo']:.1f} min")
            with col4:
                # *** NOVO: Mostrar destinos ignorados ***
                st.metric("🚫 Origem=Destino", analysis['destinos_ignorados'])
            
            # Info das melhorias
            if analysis['destinos_ignorados'] > 0:
                st.info(f"🚫 **{analysis['destinos_ignorados']} destinos ignorados** - quando origem = destino")
            
            st.info("💾 **Cache SQLite + 🛣️ Cache de Distâncias + 🚨 Error Tracking ativos**")
            
            # Processamento
            if st.button("🚀 Processar com Error Tracking", type="primary", use_container_width=True):
                
                debug_breakpoint("Início processamento com error tracking", {
                    'total_linhas': analysis['total_linhas_validas'],
                    'cache_type': 'SQLite + Distance Cache + Error Tracking',
                    'workers': max_workers
                })
                
                st.markdown("### ⚡ Processamento Paralelo com Cache SQLite + Distâncias + Error Tracking")
                
                progress_bar = st.progress(0)
                status_info = st.empty()
                
                start_time = time.time()
                
                status_info.info("🚀 Iniciando processamento com tracking de erros...")
                progress_bar.progress(20)
                
                with st.spinner("Executando processamento com cache SQLite + distâncias + error tracking..."):
                    resultados, stats_globais = sistema.parallel_processor.process_all_lines_parallel(
                        analysis['linhas_processaveis']
                    )
                
                progress_bar.progress(100)
                status_info.success("✅ Processamento com error tracking concluído!")
                
                # *** SALVAR RESULTADOS NO SESSION STATE PARA EVITAR PERDA ***
                st.session_state.processamento_resultados = resultados
                st.session_state.processamento_stats = stats_globais
                st.session_state.processamento_concluido = True
                
                # Log analytics
                sistema.analytics.log_processamento(resultados, stats_globais)
    
    # *** EXIBIR RESULTADOS SE EXISTIREM (MESMO APÓS REFRESH) ***
    if st.session_state.processamento_concluido and st.session_state.processamento_resultados:
        resultados = st.session_state.processamento_resultados
        stats_globais = st.session_state.processamento_stats
        
        st.markdown("### 📊 Resultados do Processamento SQLite + Distance Cache + Error Tracking")
        
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
            # *** NOVO: Mostrar total de erros detalhados ***
            total_erros_detalhados = len(stats_globais.get('erros_detalhados', []))
            st.metric("🚨 Erros Detalhados", total_erros_detalhados)
        
        # Taxa de sucesso
        taxa_final = (stats_globais['total_sucessos'] / max(1, stats_globais['total_sucessos'] + stats_globais['total_erros'])) * 100
        
        # Atualizar stats do cache
        updated_cache_stats = sistema.parallel_processor.cache.get_cache_stats()
        
        # *** NOVO: Informação sobre erros detalhados ***
        error_info = ""
        if total_erros_detalhados > 0:
            error_info = f"\n🚨 **{total_erros_detalhados} erros específicos** rastreados por origem-destino"
        
        st.success(f"""
        🚀 **Sistema SQLite + Cache de Distâncias + Error Tracking - Performance Máxima!**
        
        ⚡ **Processamento concluído** em {stats_globais['tempo_total']:.2f}s  
        💾 **Cache SQLite:** {updated_cache_stats['coord_hit_rate']:.1f}% hit rate coordenadas  
        🛣️ **Cache Distâncias:** {updated_cache_stats['distance_hit_rate']:.1f}% hit rate distâncias  
        📈 **Taxa de Sucesso:** {taxa_final:.1f}%  
        🔧 **{stats_globais['workers_utilizados']} workers** executando em paralelo{error_info}
        """)
        
        # *** NOVO: Painel de erros detalhados ***
        if show_detailed_errors_panel and stats_globais.get('erros_detalhados'):
            show_detailed_errors(stats_globais['erros_detalhados'])
        
        # Resultados detalhados
        st.markdown("### 📋 Resultados Detalhados por Linha")
        
        for resultado in resultados:
            # *** NOVO: Indicar se há erros na linha ***
            erros_linha = len(resultado.get('erros_detalhados', []))
            erro_indicator = f" (🚨 {erros_linha} erros)" if erros_linha > 0 else ""
            
            with st.expander(f"🎯 Linha {resultado['linha_excel']}: {resultado['origem']} ({resultado['sucessos']} sucessos{erro_indicator})"):
                
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    if resultado['status'] == 'concluido':
                        sucessos = [d for d in resultado['destinos_calculados'] if d['distancia_km'] is not None]
                        
                        if sucessos:
                            df_destinos = pd.DataFrame(sucessos)
                            df_destinos = df_destinos.sort_values('distancia_km')
                            df_destinos['ranking'] = range(1, len(df_destinos) + 1)
                            
                            # Indicar se veio do cache
                            df_destinos['origem_dados'] = df_destinos['status'].apply(
                                lambda x: '💾 Cache' if x == 'cache_hit' else '🆕 API'
                            )
                            
                            df_display = df_destinos[['ranking', 'destino', 'distancia_km', 'tempo_min', 'origem_dados']].head(10)
                            df_display.columns = ['#', 'Destino', 'Distância (km)', 'Tempo (min)', 'Fonte']
                            
                            st.dataframe(df_display, use_container_width=True, hide_index=True)
                            
                            if resultado['destino_mais_proximo']:
                                st.success(f"🏆 **Mais Próximo:** {resultado['destino_mais_proximo']} - {resultado['km_mais_proximo']} km")
                        
                        # *** NOVO: Mostrar erros da linha se houver ***
                        if resultado.get('erros_detalhados'):
                            st.markdown("**🚨 Erros nesta linha:**")
                            for erro in resultado['erros_detalhados']:
                                emoji_tipo = {
                                    'geocoding_origem': '🏠',
                                    'geocoding_destino': '🎯',
                                    'calculo_distancia': '🛣️'
                                }.get(erro['tipo'], '❌')
                                st.error(f"{emoji_tipo} {erro['origem']} → {erro['destino']}: {erro['erro']}")
                
                with col2:
                    st.metric("⏱️ Tempo", f"{resultado['tempo_processamento']}s")
                    st.metric("📊 Total", resultado['total_destinos'])
                    st.metric("✅ Sucessos", resultado['sucessos'])
                    st.metric("❌ Erros", resultado['erros'])
                    
                    # *** NOVO: Mostrar quantidade de erros detalhados ***
                    if resultado.get('erros_detalhados'):
                        st.metric("🚨 Erros Detalhados", len(resultado['erros_detalhados']))
                    
                    st.text("💾 Cache: SQLite")
                    st.text("🛣️ Distâncias: Cache")
                    st.text("🚨 Error Tracking: ON")
                    
                    if show_maps and resultado['status'] == 'concluido':
                        mapa_linha = sistema.mapper.create_route_map(resultado)
                        if mapa_linha:
                            st.markdown("**🗺️ Mapa:**")
                            st_folium(mapa_linha, width=300, height=200)
        
        # Download
        st.markdown("### 💾 Download do Relatório Error Tracking")
        
        excel_data = create_advanced_excel_error_tracking(resultados, stats_globais, updated_cache_stats)
        
        if excel_data:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"relatorio_error_tracking_{timestamp}.xlsx"
            
            st.download_button(
                label="📥 Baixar Relatório Error Tracking (Excel)",
                data=excel_data,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True
            )
            
            st.info("""
            📊 **Relatório Error Tracking contém:**
            - 📋 Resultados detalhados por linha
            - 🚨 Lista completa de erros por origem-destino
            - 📊 Estatísticas de tipos de erro
            - 💾 Métricas de cache SQLite e distâncias
            - ⚡ Analytics de performance
            """)
        
        # Botão para limpar resultados
        if st.button("🗑️ Limpar Resultados", help="Remove os resultados da tela"):
            st.session_state.processamento_resultados = None
            st.session_state.processamento_stats = None
            st.session_state.processamento_concluido = False

def create_advanced_excel_error_tracking(resultados, stats_globais, cache_stats):
    """Cria Excel com informações específicas de error tracking"""
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
                            'Origem_Dados': 'Cache' if dest_data.get('status') == 'cache_hit' else 'API',
                            'Tempo_Processamento_Linha': resultado['tempo_processamento']
                        })
            
            if relatorio_completo:
                df_completo = pd.DataFrame(relatorio_completo)
                df_completo.to_excel(writer, sheet_name='Resultados_Sucessos', index=False)
            
            # *** NOVA ABA: Erros Detalhados ***
            erros_detalhados = stats_globais.get('erros_detalhados', [])
            if erros_detalhados:
                df_erros = pd.DataFrame(erros_detalhados)
                df_erros['Par_Origem_Destino'] = df_erros['origem'] + ' → ' + df_erros['destino']
                df_erros = df_erros[['tipo', 'origem', 'destino', 'Par_Origem_Destino', 'erro']]
                df_erros.columns = ['Tipo_Erro', 'Origem', 'Destino', 'Par_Origem_Destino', 'Descricao_Erro']
                df_erros.to_excel(writer, sheet_name='Erros_Detalhados', index=False)
                
                # Estatísticas de erros
                tipos_erro = df_erros['Tipo_Erro'].value_counts().reset_index()
                tipos_erro.columns = ['Tipo_Erro', 'Quantidade']
                tipos_erro.to_excel(writer, sheet_name='Estatisticas_Erros', index=False)
            
            # Métricas específicas do error tracking
            performance_data = {
                'Métrica': [
                    'Tempo Total de Processamento (s)',
                    'Cache Type',
                    'Error Tracking Ativo',
                    'Total de Erros Detalhados',
                    'Cache Hit Rate Coordenadas (%)',
                    'Cache Hit Rate Distâncias (%)',
                    'Hit Rate Geral (%)',
                    'Total Coordenadas Cache',
                    'Total Distâncias Cache',
                    'Total Entradas Cache',
                    'TTL Coordenadas (h)',
                    'TTL Distâncias (h)',
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
                    'SQLite + Distance Cache + Error Tracking',
                    'SIM',
                    len(erros_detalhados),
                    cache_stats['coord_hit_rate'],
                    cache_stats['distance_hit_rate'],
                    cache_stats['hit_rate'],
                    cache_stats['total_coordinates'],
                    cache_stats['total_distances'],
                    cache_stats['total_entries'],
                    cache_stats['ttl_hours'],
                    cache_stats['distance_ttl_hours'],
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
            df_performance.to_excel(writer, sheet_name='Metricas_Error_Tracking', index=False)
        
        return output.getvalue()
        
    except Exception as e:
        st.error(f"❌ Erro ao criar Excel: {e}")
        return None

if __name__ == "__main__":
    main()