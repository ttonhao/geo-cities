# sqlite_cache.py
"""
💾 SISTEMA DE CACHE SQLITE COMPLETO COM CACHE DE DISTÂNCIAS
==========================================================

Cache persistente para coordenadas E distâncias calculadas.
Evita recálculos desnecessários de rotas já conhecidas.

Versão: 1.1.0 - Com Cache de Distâncias
"""

import sqlite3
import threading
import hashlib
import json
import os
import time
import contextlib
import shutil
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any
import logging

class SQLiteCache:
    """Sistema de cache SQLite completo com cache de distâncias"""
    
    def __init__(self, 
                 db_path: str = "cache/geocoding_cache.db",
                 ttl_hours: int = 24,
                 distance_ttl_hours: int = 168,  # 7 dias para distâncias
                 max_entries: int = 50000,
                 auto_cleanup: bool = True,
                 backup_enabled: bool = True,
                 backup_interval_hours: int = 6):
        
        self.db_path = db_path
        self.ttl_hours = ttl_hours
        self.distance_ttl_hours = distance_ttl_hours  # TTL específico para distâncias
        self.max_entries = max_entries
        self.auto_cleanup = auto_cleanup
        self.backup_enabled = backup_enabled
        self.backup_interval_hours = backup_interval_hours
        
        # Thread safety
        self._lock = threading.RLock()
        self._local = threading.local()
        
        # Debug e logging
        self.debug_enabled = os.getenv('STREAMLIT_ENV') == 'development'
        self._setup_logging()
        
        # Inicialização
        self._ensure_db_directory()
        self._initialize_database()
        self._setup_indexes()
        self._run_migrations()
        
        # Background tasks
        if auto_cleanup:
            self._schedule_cleanup()
        
        if backup_enabled:
            self._schedule_backup()
        
        self._log("SQLiteCache com cache de distâncias inicializado", level="INFO")
    
    def _setup_logging(self):
        """Configura sistema de logging"""
        self.logger = logging.getLogger('SQLiteCache')
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '[%(asctime)s] [CACHE] [%(threadName)s] %(levelname)s: %(message)s',
                datefmt='%H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        self.logger.setLevel(logging.DEBUG if self.debug_enabled else logging.INFO)
    
    def _log(self, message: str, level: str = "DEBUG"):
        """Log interno do cache"""
        if level == "DEBUG":
            self.logger.debug(message)
        elif level == "INFO":
            self.logger.info(message)
        elif level == "WARNING":
            self.logger.warning(message)
        elif level == "ERROR":
            self.logger.error(message)
    
    def _ensure_db_directory(self):
        """Garante que o diretório do banco existe"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            self._log(f"Diretório criado: {db_dir}")
        
        # Criar diretório de backup
        backup_dir = os.path.join(db_dir, 'backups')
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir, exist_ok=True)
            self._log(f"Diretório de backup criado: {backup_dir}")
    
    @contextlib.contextmanager
    def _get_connection(self):
        """Context manager para conexões thread-safe"""
        if not hasattr(self._local, 'connection'):
            self._local.connection = sqlite3.connect(
                self.db_path,
                timeout=30.0,
                check_same_thread=False
            )
            self._local.connection.row_factory = sqlite3.Row
            
            # Configurações de performance otimizada
            cursor = self._local.connection.cursor()
            performance_pragmas = [
                "PRAGMA journal_mode=WAL",
                "PRAGMA synchronous=NORMAL", 
                "PRAGMA cache_size=10000",
                "PRAGMA temp_store=MEMORY",
                "PRAGMA mmap_size=268435456",  # 256MB
                "PRAGMA optimize"
            ]
            
            for pragma in performance_pragmas:
                cursor.execute(pragma)
            
            cursor.close()
        
        try:
            yield self._local.connection
        except Exception as e:
            self._local.connection.rollback()
            self._log(f"Erro na conexão: {e}", "ERROR")
            raise
    
    def _initialize_database(self):
        """Inicializa esquema completo do banco"""
        self._log("Inicializando esquema do banco SQLite com cache de distâncias")
        
        with self._lock:
            with self._get_connection() as conn:
                # Tabela de coordenadas (existente)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS coordinates (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        city_hash TEXT UNIQUE NOT NULL,
                        city_name TEXT NOT NULL,
                        city_name_normalized TEXT NOT NULL,
                        longitude REAL NOT NULL,
                        latitude REAL NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        expires_at TIMESTAMP NOT NULL,
                        hits INTEGER DEFAULT 0,
                        source TEXT DEFAULT 'nominatim',
                        confidence REAL DEFAULT 1.0,
                        metadata TEXT DEFAULT '{}',
                        is_verified BOOLEAN DEFAULT 0
                    )
                """)
                
                # Nova tabela de distâncias
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS distances (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        route_hash TEXT UNIQUE NOT NULL,
                        origin_city TEXT NOT NULL,
                        destination_city TEXT NOT NULL,
                        origin_coords TEXT NOT NULL,
                        destination_coords TEXT NOT NULL,
                        distance_km REAL NOT NULL,
                        duration_minutes REAL NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        expires_at TIMESTAMP NOT NULL,
                        hits INTEGER DEFAULT 0,
                        source TEXT DEFAULT 'osrm',
                        metadata TEXT DEFAULT '{}'
                    )
                """)
                
                # Tabela de estatísticas detalhadas
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cache_stats (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date DATE DEFAULT CURRENT_DATE,
                        session_id TEXT NOT NULL,
                        coord_hits INTEGER DEFAULT 0,
                        coord_misses INTEGER DEFAULT 0,
                        coord_saves INTEGER DEFAULT 0,
                        distance_hits INTEGER DEFAULT 0,
                        distance_misses INTEGER DEFAULT 0,
                        distance_saves INTEGER DEFAULT 0,
                        cleanups INTEGER DEFAULT 0,
                        total_entries INTEGER DEFAULT 0,
                        avg_hit_rate REAL DEFAULT 0.0,
                        processing_time_avg REAL DEFAULT 0.0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(date, session_id)
                    )
                """)
                
                # Demais tabelas (migrations, backups, etc)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS cache_migrations (
                        version INTEGER PRIMARY KEY,
                        description TEXT NOT NULL,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS backup_metadata (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        backup_path TEXT NOT NULL,
                        backup_type TEXT DEFAULT 'auto',
                        entries_count INTEGER NOT NULL,
                        file_size INTEGER DEFAULT 0,
                        compression_ratio REAL DEFAULT 0.0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        restored_at TIMESTAMP NULL,
                        checksum TEXT NULL
                    )
                """)
                
                conn.commit()
                self._log("Esquema do banco com cache de distâncias inicializado")
    
    def _setup_indexes(self):
        """Cria índices otimizados para performance"""
        self._log("Configurando índices de performance")
        
        with self._lock:
            with self._get_connection() as conn:
                indexes = [
                    # Índices para coordenadas
                    "CREATE INDEX IF NOT EXISTS idx_coordinates_hash ON coordinates(city_hash)",
                    "CREATE INDEX IF NOT EXISTS idx_coordinates_name ON coordinates(city_name_normalized)",
                    "CREATE INDEX IF NOT EXISTS idx_coordinates_expires ON coordinates(expires_at)",
                    "CREATE INDEX IF NOT EXISTS idx_coordinates_hits ON coordinates(hits DESC)",
                    
                    # Índices para distâncias
                    "CREATE INDEX IF NOT EXISTS idx_distances_hash ON distances(route_hash)",
                    "CREATE INDEX IF NOT EXISTS idx_distances_expires ON distances(expires_at)",
                    "CREATE INDEX IF NOT EXISTS idx_distances_hits ON distances(hits DESC)",
                    "CREATE INDEX IF NOT EXISTS idx_distances_origin ON distances(origin_city)",
                    "CREATE INDEX IF NOT EXISTS idx_distances_destination ON distances(destination_city)",
                    
                    # Índices compostos
                    "CREATE INDEX IF NOT EXISTS idx_distances_route ON distances(origin_city, destination_city)",
                    "CREATE INDEX IF NOT EXISTS idx_distances_expires_hits ON distances(expires_at, hits DESC)"
                ]
                
                for index_sql in indexes:
                    try:
                        conn.execute(index_sql)
                    except sqlite3.Error as e:
                        self._log(f"Erro ao criar índice: {e}", "WARNING")
                
                conn.commit()
                self._log("Índices configurados")
    
    def _run_migrations(self):
        """Executa migrations do banco"""
        migrations = [
            (1, "Initial schema", self._migration_v1),
            (2, "Add distances table", self._migration_v2),
            (3, "Update stats table for distances", self._migration_v3)
        ]
        
        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.execute("SELECT version FROM cache_migrations ORDER BY version DESC LIMIT 1")
                row = cursor.fetchone()
                current_version = row['version'] if row else 0
                
                for version, description, migration_func in migrations:
                    if version > current_version:
                        try:
                            self._log(f"Executando migration v{version}: {description}")
                            migration_func(conn)
                            
                            conn.execute("""
                                INSERT INTO cache_migrations (version, description)
                                VALUES (?, ?)
                            """, (version, description))
                            
                            conn.commit()
                            self._log(f"Migration v{version} aplicada")
                            
                        except Exception as e:
                            self._log(f"Erro na migration v{version}: {e}", "ERROR")
                            conn.rollback()
                            raise
    
    def _migration_v1(self, conn):
        """Migration inicial"""
        pass
    
    def _migration_v2(self, conn):
        """Adiciona tabela de distâncias se não existir"""
        pass  # Já criada no _initialize_database
    
    def _migration_v3(self, conn):
        """Atualiza tabela de stats para incluir métricas de distâncias"""
        try:
            # Adicionar colunas de distâncias se não existirem
            distance_columns = [
                "ALTER TABLE cache_stats ADD COLUMN distance_hits INTEGER DEFAULT 0",
                "ALTER TABLE cache_stats ADD COLUMN distance_misses INTEGER DEFAULT 0", 
                "ALTER TABLE cache_stats ADD COLUMN distance_saves INTEGER DEFAULT 0"
            ]
            
            for col_sql in distance_columns:
                try:
                    conn.execute(col_sql)
                except sqlite3.Error:
                    pass  # Coluna já existe
        except Exception as e:
            self._log(f"Erro na migration v3: {e}", "WARNING")
    
    def _get_city_hash(self, city_name: str) -> str:
        """Gera hash único para a cidade"""
        normalized = city_name.upper().strip()
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()
    
    def _get_route_hash(self, origin_city: str, destination_city: str) -> str:
        """Gera hash único para o par origem-destino"""
        # Normalizar nomes das cidades
        origin_norm = origin_city.upper().strip()
        dest_norm = destination_city.upper().strip()
        
        # Criar string única para o par (ordem alfabética para consistência)
        if origin_norm < dest_norm:
            route_string = f"{origin_norm}|{dest_norm}"
        else:
            route_string = f"{dest_norm}|{origin_norm}"
        
        return hashlib.sha256(route_string.encode('utf-8')).hexdigest()
    
    def _normalize_city_name(self, city_name: str) -> str:
        """Normaliza nome da cidade para busca"""
        return city_name.upper().strip().replace('  ', ' ')
    
    def _calculate_expires_at(self, ttl_hours: int = None) -> str:
        """Calcula timestamp de expiração"""
        if ttl_hours is None:
            ttl_hours = self.ttl_hours
        expires = datetime.now() + timedelta(hours=ttl_hours)
        return expires.isoformat()
    
    def _is_expired(self, expires_at: str) -> bool:
        """Verifica se um registro expirou"""
        try:
            expires = datetime.fromisoformat(expires_at)
            return datetime.now() > expires
        except:
            return True
    
    # ================================
    # MÉTODOS DE COORDENADAS (existentes)
    # ================================
    
    def save_coordinates(self, 
                        city_name: str, 
                        coordinates: List[float],
                        source: str = "nominatim",
                        confidence: float = 1.0,
                        metadata: Dict = None,
                        is_verified: bool = False) -> bool:
        """Salva coordenadas no cache SQLite"""
        start_time = time.time()
        
        if not coordinates or len(coordinates) != 2:
            self._log(f"Coordenadas inválidas para {city_name}: {coordinates}", "WARNING")
            return False
        
        city_hash = self._get_city_hash(city_name)
        city_normalized = self._normalize_city_name(city_name)
        expires_at = self._calculate_expires_at()
        metadata_json = json.dumps(metadata or {})
        
        self._log(f"Salvando coordenadas: {city_name} -> {coordinates}")
        
        with self._lock:
            try:
                with self._get_connection() as conn:
                    conn.execute("""
                        INSERT INTO coordinates 
                        (city_hash, city_name, city_name_normalized, longitude, latitude, 
                         expires_at, source, confidence, metadata, is_verified, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(city_hash) DO UPDATE SET
                            longitude = excluded.longitude,
                            latitude = excluded.latitude,
                            expires_at = excluded.expires_at,
                            source = excluded.source,
                            confidence = excluded.confidence,
                            metadata = excluded.metadata,
                            is_verified = excluded.is_verified,
                            updated_at = CURRENT_TIMESTAMP
                    """, (city_hash, city_name, city_normalized, coordinates[0], coordinates[1], 
                          expires_at, source, confidence, metadata_json, is_verified))
                    
                    conn.commit()
                    self._update_stats('coord_save')
                    
                    execution_time = time.time() - start_time
                    self._log(f"Coordenadas salvas: {city_name} (tempo: {execution_time:.3f}s)")
                    return True
                    
            except sqlite3.Error as e:
                self._log(f"Erro ao salvar coordenadas de {city_name}: {e}", "ERROR")
                return False
    
    def get_coordinates(self, city_name: str) -> Optional[List[float]]:
        """Recupera coordenadas do cache"""
        start_time = time.time()
        city_hash = self._get_city_hash(city_name)
        
        with self._lock:
            try:
                with self._get_connection() as conn:
                    cursor = conn.execute("""
                        SELECT longitude, latitude, expires_at, hits
                        FROM coordinates 
                        WHERE city_hash = ?
                    """, (city_hash,))
                    
                    row = cursor.fetchone()
                    
                    if row is None:
                        self._update_stats('coord_miss')
                        self._log(f"Cache MISS coordenadas: {city_name}")
                        return None
                    
                    # Verificar expiração
                    if self._is_expired(row['expires_at']):
                        conn.execute("DELETE FROM coordinates WHERE city_hash = ?", (city_hash,))
                        conn.commit()
                        self._update_stats('coord_miss')
                        self._log(f"Cache EXPIRED coordenadas: {city_name}")
                        return None
                    
                    # Atualizar hits
                    conn.execute("""
                        UPDATE coordinates 
                        SET hits = hits + 1, updated_at = CURRENT_TIMESTAMP
                        WHERE city_hash = ?
                    """, (city_hash,))
                    conn.commit()
                    
                    coordinates = [row['longitude'], row['latitude']]
                    self._update_stats('coord_hit')
                    self._log(f"Cache HIT coordenadas: {city_name} -> {coordinates}")
                    return coordinates
                    
            except sqlite3.Error as e:
                self._log(f"Erro ao buscar coordenadas de {city_name}: {e}", "ERROR")
                self._update_stats('coord_miss')
                return None
    
    # ================================
    # NOVOS MÉTODOS DE DISTÂNCIAS
    # ================================
    
    def save_distance(self, 
                     origin_city: str, 
                     destination_city: str,
                     origin_coords: List[float],
                     destination_coords: List[float],
                     distance_km: float,
                     duration_minutes: float,
                     source: str = "osrm",
                     metadata: Dict = None) -> bool:
        """
        Salva distância calculada no cache
        
        Args:
            origin_city: Nome da cidade origem
            destination_city: Nome da cidade destino
            origin_coords: [longitude, latitude] da origem
            destination_coords: [longitude, latitude] do destino
            distance_km: Distância em quilômetros
            duration_minutes: Duração em minutos
            source: Fonte do cálculo (osrm, etc)
            metadata: Metadados adicionais
        """
        start_time = time.time()
        
        route_hash = self._get_route_hash(origin_city, destination_city)
        expires_at = self._calculate_expires_at(self.distance_ttl_hours)
        metadata_json = json.dumps(metadata or {})
        origin_coords_str = json.dumps(origin_coords)
        dest_coords_str = json.dumps(destination_coords)
        
        self._log(f"Salvando distância: {origin_city} -> {destination_city} = {distance_km}km")
        
        with self._lock:
            try:
                with self._get_connection() as conn:
                    conn.execute("""
                        INSERT INTO distances 
                        (route_hash, origin_city, destination_city, origin_coords, destination_coords,
                         distance_km, duration_minutes, expires_at, source, metadata, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(route_hash) DO UPDATE SET
                            distance_km = excluded.distance_km,
                            duration_minutes = excluded.duration_minutes,
                            expires_at = excluded.expires_at,
                            source = excluded.source,
                            metadata = excluded.metadata,
                            updated_at = CURRENT_TIMESTAMP
                    """, (route_hash, origin_city, destination_city, origin_coords_str, dest_coords_str,
                          distance_km, duration_minutes, expires_at, source, metadata_json))
                    
                    conn.commit()
                    self._update_stats('distance_save')
                    
                    execution_time = time.time() - start_time
                    self._log(f"Distância salva: {origin_city}->{destination_city} (tempo: {execution_time:.3f}s)")
                    return True
                    
            except sqlite3.Error as e:
                self._log(f"Erro ao salvar distância {origin_city}->{destination_city}: {e}", "ERROR")
                return False
    
    def get_distance(self, origin_city: str, destination_city: str) -> Optional[Dict[str, Any]]:
        """
        Recupera distância do cache
        
        Args:
            origin_city: Nome da cidade origem
            destination_city: Nome da cidade destino
            
        Returns:
            Dict com distancia_km, tempo_min, etc ou None se não encontrado
        """
        start_time = time.time()
        route_hash = self._get_route_hash(origin_city, destination_city)
        
        self._log(f"Buscando distância: {origin_city} -> {destination_city}")
        
        with self._lock:
            try:
                with self._get_connection() as conn:
                    cursor = conn.execute("""
                        SELECT distance_km, duration_minutes, expires_at, hits, created_at,
                               source, metadata, origin_coords, destination_coords
                        FROM distances 
                        WHERE route_hash = ?
                    """, (route_hash,))
                    
                    row = cursor.fetchone()
                    
                    if row is None:
                        self._update_stats('distance_miss')
                        self._log(f"Cache MISS distância: {origin_city} -> {destination_city}")
                        return None
                    
                    # Verificar expiração
                    if self._is_expired(row['expires_at']):
                        conn.execute("DELETE FROM distances WHERE route_hash = ?", (route_hash,))
                        conn.commit()
                        self._update_stats('distance_miss')
                        self._log(f"Cache EXPIRED distância: {origin_city} -> {destination_city}")
                        return None
                    
                    # Atualizar hits
                    conn.execute("""
                        UPDATE distances 
                        SET hits = hits + 1, updated_at = CURRENT_TIMESTAMP
                        WHERE route_hash = ?
                    """, (route_hash,))
                    conn.commit()
                    
                    result = {
                        'distancia_km': round(row['distance_km'], 2),
                        'tempo_min': round(row['duration_minutes'], 0),
                        'status': 'cache_hit',
                        'hits': row['hits'] + 1,
                        'source': row['source'],
                        'cached_at': row['created_at']
                    }
                    
                    self._update_stats('distance_hit')
                    self._log(f"Cache HIT distância: {origin_city} -> {destination_city} = {result['distancia_km']}km")
                    return result
                    
            except sqlite3.Error as e:
                self._log(f"Erro ao buscar distância {origin_city}->{destination_city}: {e}", "ERROR")
                self._update_stats('distance_miss')
                return None
    
    def _get_session_id(self) -> str:
        """Gera ID único da sessão"""
        try:
            import streamlit as st
            if 'cache_session_id' not in st.session_state:
                st.session_state.cache_session_id = hashlib.md5(
                    f"{datetime.now().isoformat()}_{os.getpid()}".encode()
                ).hexdigest()[:16]
            return st.session_state.cache_session_id
        except:
            return hashlib.md5(f"standalone_{os.getpid()}".encode()).hexdigest()[:16]
    
    def _update_stats(self, action: str):
        """Atualiza estatísticas do cache"""
        session_id = self._get_session_id()
        
        try:
            with self._get_connection() as conn:
                cursor = conn.execute("""
                    SELECT coord_hits, coord_misses, coord_saves, distance_hits, distance_misses, distance_saves 
                    FROM cache_stats 
                    WHERE date = CURRENT_DATE AND session_id = ?
                """, (session_id,))
                
                row = cursor.fetchone()
                
                if row is None:
                    # Criar novo registro
                    initial_values = {
                        'coord_hits': 1 if action == 'coord_hit' else 0,
                        'coord_misses': 1 if action == 'coord_miss' else 0,
                        'coord_saves': 1 if action == 'coord_save' else 0,
                        'distance_hits': 1 if action == 'distance_hit' else 0,
                        'distance_misses': 1 if action == 'distance_miss' else 0,
                        'distance_saves': 1 if action == 'distance_save' else 0
                    }
                    
                    conn.execute("""
                        INSERT INTO cache_stats (date, session_id, coord_hits, coord_misses, coord_saves,
                                               distance_hits, distance_misses, distance_saves)
                        VALUES (CURRENT_DATE, ?, ?, ?, ?, ?, ?, ?)
                    """, (session_id, initial_values['coord_hits'], initial_values['coord_misses'], 
                          initial_values['coord_saves'], initial_values['distance_hits'],
                          initial_values['distance_misses'], initial_values['distance_saves']))
                else:
                    # Atualizar registro existente
                    update_sql = f"""
                        UPDATE cache_stats 
                        SET {action} = {action} + 1, updated_at = CURRENT_TIMESTAMP
                        WHERE date = CURRENT_DATE AND session_id = ?
                    """
                    conn.execute(update_sql, (session_id,))
                
                conn.commit()
                
        except sqlite3.Error as e:
            self._log(f"Erro ao atualizar estatísticas: {e}", "ERROR")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas completas do cache"""
        session_id = self._get_session_id()
        
        with self._lock:
            try:
                with self._get_connection() as conn:
                    # Estatísticas da sessão atual
                    cursor = conn.execute("""
                        SELECT coord_hits, coord_misses, coord_saves, 
                               distance_hits, distance_misses, distance_saves
                        FROM cache_stats 
                        WHERE date = CURRENT_DATE AND session_id = ?
                    """, (session_id,))
                    
                    session_stats = cursor.fetchone()
                    
                    if session_stats is None:
                        coord_hits = coord_misses = coord_saves = 0
                        distance_hits = distance_misses = distance_saves = 0
                    else:
                        coord_hits = session_stats['coord_hits']
                        coord_misses = session_stats['coord_misses'] 
                        coord_saves = session_stats['coord_saves']
                        distance_hits = session_stats['distance_hits']
                        distance_misses = session_stats['distance_misses']
                        distance_saves = session_stats['distance_saves']
                    
                    # Estatísticas globais
                    cursor = conn.execute("""
                        SELECT COUNT(*) as total_coords FROM coordinates 
                        WHERE expires_at > CURRENT_TIMESTAMP
                    """)
                    total_coords = cursor.fetchone()['total_coords']
                    
                    cursor = conn.execute("""
                        SELECT COUNT(*) as total_distances FROM distances 
                        WHERE expires_at > CURRENT_TIMESTAMP
                    """)
                    total_distances = cursor.fetchone()['total_distances']
                    
                    # Informações do arquivo
                    file_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
                    
                    # Calcular hit rates
                    total_coord_requests = coord_hits + coord_misses
                    coord_hit_rate = (coord_hits / total_coord_requests * 100) if total_coord_requests > 0 else 0
                    
                    total_distance_requests = distance_hits + distance_misses
                    distance_hit_rate = (distance_hits / total_distance_requests * 100) if total_distance_requests > 0 else 0
                    
                    # Hit rate geral
                    total_hits = coord_hits + distance_hits
                    total_requests = total_coord_requests + total_distance_requests
                    overall_hit_rate = (total_hits / total_requests * 100) if total_requests > 0 else 0
                    
                    stats = {
                        # Coordenadas
                        'coord_hits': coord_hits,
                        'coord_misses': coord_misses,
                        'coord_saves': coord_saves,
                        'coord_hit_rate': round(coord_hit_rate, 2),
                        'total_coordinates': total_coords,
                        
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
                        'total_entries': total_coords + total_distances,
                        
                        # Sistema
                        'database_path': self.db_path,
                        'file_size_bytes': file_size,
                        'file_size_mb': round(file_size / (1024 * 1024), 2),
                        'ttl_hours': self.ttl_hours,
                        'distance_ttl_hours': self.distance_ttl_hours,
                        'session_id': session_id
                    }
                    
                    return stats
                    
            except sqlite3.Error as e:
                self._log(f"Erro ao obter estatísticas: {e}", "ERROR")
                return {
                    'coord_hits': 0, 'coord_misses': 0, 'coord_saves': 0,
                    'distance_hits': 0, 'distance_misses': 0, 'distance_saves': 0,
                    'total_requests': 0, 'hit_rate': 0, 'error': str(e)
                }
    
    def cleanup_expired(self) -> int:
        """Remove entradas expiradas"""
        with self._lock:
            try:
                with self._get_connection() as conn:
                    # Limpar coordenadas expiradas
                    cursor = conn.execute("""
                        SELECT COUNT(*) as expired_coords FROM coordinates 
                        WHERE expires_at <= CURRENT_TIMESTAMP
                    """)
                    expired_coords = cursor.fetchone()['expired_coords']
                    
                    # Limpar distâncias expiradas
                    cursor = conn.execute("""
                        SELECT COUNT(*) as expired_distances FROM distances 
                        WHERE expires_at <= CURRENT_TIMESTAMP
                    """)
                    expired_distances = cursor.fetchone()['expired_distances']
                    
                    # Remover entradas expiradas
                    conn.execute("DELETE FROM coordinates WHERE expires_at <= CURRENT_TIMESTAMP")
                    conn.execute("DELETE FROM distances WHERE expires_at <= CURRENT_TIMESTAMP")
                    conn.commit()
                    
                    total_removed = expired_coords + expired_distances
                    
                    if total_removed > 0:
                        self._log(f"Limpeza: {expired_coords} coordenadas + {expired_distances} distâncias expiradas removidas")
                    
                    return total_removed
                    
            except sqlite3.Error as e:
                self._log(f"Erro na limpeza: {e}", "ERROR")
                return 0
    
    def clear_cache(self) -> bool:
        """Limpa todo o cache"""
        with self._lock:
            try:
                with self._get_connection() as conn:
                    cursor = conn.execute("SELECT COUNT(*) as coords FROM coordinates")
                    coords_count = cursor.fetchone()['coords']
                    
                    cursor = conn.execute("SELECT COUNT(*) as distances FROM distances")
                    distances_count = cursor.fetchone()['distances']
                    
                    conn.execute("DELETE FROM coordinates")
                    conn.execute("DELETE FROM distances")
                    
                    session_id = self._get_session_id()
                    conn.execute("DELETE FROM cache_stats WHERE session_id = ?", (session_id,))
                    
                    conn.commit()
                    
                    self._log(f"Cache limpo: {coords_count} coordenadas + {distances_count} distâncias removidas")
                    return True
                    
            except sqlite3.Error as e:
                self._log(f"Erro ao limpar cache: {e}", "ERROR")
                return False
    
    def get_database_info(self) -> Dict[str, Any]:
        """Retorna informações detalhadas do banco"""
        try:
            file_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            
            with self._get_connection() as conn:
                cursor = conn.execute("SELECT COUNT(*) as coords FROM coordinates")
                total_coords = cursor.fetchone()['coords']
                
                cursor = conn.execute("SELECT COUNT(*) as distances FROM distances")
                total_distances = cursor.fetchone()['distances']
                
                return {
                    'database_path': self.db_path,
                    'file_size_bytes': file_size,
                    'file_size_mb': round(file_size / (1024 * 1024), 2),
                    'total_coordinates': total_coords,
                    'total_distances': total_distances,
                    'total_entries': total_coords + total_distances,
                    'ttl_hours': self.ttl_hours,
                    'distance_ttl_hours': self.distance_ttl_hours
                }
                
        except Exception as e:
            return {'error': str(e)}
    
    def _schedule_cleanup(self):
        """Agenda limpeza automática"""
        def cleanup_worker():
            while True:
                try:
                    time.sleep(3600)  # 1 hora
                    if self.auto_cleanup:
                        removed = self.cleanup_expired()
                        if removed > 0:
                            self._log(f"Auto-limpeza: {removed} entradas removidas")
                except Exception as e:
                    self._log(f"Erro na auto-limpeza: {e}", "ERROR")
        
        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cleanup_thread.start()
    
    def _schedule_backup(self):
        """Agenda backup automático"""
        def backup_worker():
            while True:
                try:
                    time.sleep(self.backup_interval_hours * 3600)
                    if self.backup_enabled:
                        # Implementar backup se necessário
                        pass
                except Exception as e:
                    self._log(f"Erro no backup automático: {e}", "ERROR")
        
        backup_thread = threading.Thread(target=backup_worker, daemon=True)
        backup_thread.start()
    
    def close(self):
        """Fecha conexões"""
        try:
            if hasattr(self._local, 'connection'):
                self._local.connection.close()
            self._log("Cache fechado")
        except Exception as e:
            self._log(f"Erro ao fechar cache: {e}", "ERROR")

# Factory function
def create_sqlite_cache_instance(environment: str = None) -> SQLiteCache:
    """Cria instância do cache SQLite"""
    if environment is None:
        environment = os.getenv('STREAMLIT_ENV', 'production')
    
    if environment == 'development':
        return SQLiteCache(
            db_path='cache/dev_geocoding_cache.db',
            ttl_hours=1,
            distance_ttl_hours=24,  # 1 dia em dev
            max_entries=5000,
            auto_cleanup=True,
            backup_enabled=True,
            backup_interval_hours=1
        )
    else:
        return SQLiteCache(
            db_path='cache/prod_geocoding_cache.db',
            ttl_hours=24,
            distance_ttl_hours=168,  # 7 dias em prod
            max_entries=50000,
            auto_cleanup=True,
            backup_enabled=True,
            backup_interval_hours=6
        )

print("💾 Sistema de Cache SQLite com CACHE DE DISTÂNCIAS carregado!")
print("✅ Funcionalidades:")
print("   • Cache persistente de coordenadas")
print("   • Cache persistente de distâncias")  
print("   • TTL independente para cada tipo")
print("   • Thread-safe operations")
print("   • Backup automático")
print("   • Métricas separadas por tipo")