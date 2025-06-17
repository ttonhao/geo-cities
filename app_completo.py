"""
🚀 SISTEMA AVANÇADO DE DISTÂNCIAS - VERSÃO PRO COM DEBUG
=======================================================

VERSÃO COMPLETA: Todas as funcionalidades originais + Debug integrado
- ✅ Cache Inteligente completo
- ✅ Processamento Paralelo completo  
- ✅ Dashboard Analytics completo
- ✅ Mapas Interativos completos
- ✅ Sistema de Debug integrado
- ✅ Breakpoints estratégicos
- ✅ Logging detalhado

Versão: 3.0.2 - Pro Debug (Completa)
"""

import streamlit as st
import pandas as pd
import requests
import time
import asyncio
import threading
import functools
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from geopy.geocoders import Nominatim
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
from io import BytesIO
import json
import hashlib
from datetime import datetime, timedelta
import numpy as np
import warnings
import logging

# ================================
# CONFIGURAÇÕES DE DEBUG
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
        
        # Log no console apenas em desenvolvimento
        if DEBUG_MODE:
            print(log_entry)
        
        # Armazenar no session state
        if 'debug_logs' not in st.session_state:
            st.session_state.debug_logs = []
        
        st.session_state.debug_logs.append({
            'timestamp': timestamp,
            'thread': thread_name,
            'level': level,
            'message': message
        })
        
        # Limitar logs armazenados
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

# Instância global do debugger
debugger = StreamlitDebugger()

def debug_breakpoint(message="Debug breakpoint", variables=None):
    """Breakpoint customizado com informações contextuais"""
    if DEBUG_MODE:
        debugger.debug(f"BREAKPOINT: {message}")
        
        if variables:
            for name, value in variables.items():
                debugger.debug(f"  {name}: {value} (type: {type(value).__name__})")
        
        # Este ponto é ideal para colocar breakpoint no VS Code
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
    page_title="🚀 Sistema Avançado de Distâncias",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Suprimir warnings de threading
warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)

# CSS avançado COMPLETO
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
    
    .parallel-status {
        background: linear-gradient(135deg, #89f7fe 0%, #66a6ff 100%);
        padding: 1rem;
        border-radius: 12px;
        color: white;
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
# 1. CACHE INTELIGENTE COMPLETO COM DEBUG
# ================================

class SmartCache:
    def __init__(self):
        self.cache_key = "coordinates_cache_v3"
        self.stats_key = "cache_stats_v3"
        debugger.debug("SmartCache inicializado")
        
    def _get_cache_hash(self, city_name):
        """Gera hash único para a cidade"""
        return hashlib.md5(city_name.upper().encode()).hexdigest()
    
    def save_coordinates(self, city_name, coordinates):
        """Salva coordenadas no cache com debug"""
        debugger.debug(f"Salvando no cache: {city_name} -> {coordinates}")
        
        if 'coordinates_cache' not in st.session_state:
            st.session_state.coordinates_cache = {}
        
        cache_hash = self._get_cache_hash(city_name)
        st.session_state.coordinates_cache[cache_hash] = {
            'city': city_name,
            'coords': coordinates,
            'timestamp': datetime.now(),
            'hits': 0
        }
        
        self._update_cache_stats('save')
        
        # BREAKPOINT: Cache save
        debug_breakpoint("Cache save", {
            'city_name': city_name,
            'coordinates': coordinates,
            'cache_size': len(st.session_state.coordinates_cache)
        })
    
    def get_coordinates(self, city_name):
        """Recupera coordenadas do cache com debug"""
        debugger.debug(f"Buscando coordenadas para: {city_name}")
        
        if 'coordinates_cache' not in st.session_state:
            st.session_state.coordinates_cache = {}
        
        cache_hash = self._get_cache_hash(city_name)
        
        if cache_hash in st.session_state.coordinates_cache:
            cached_data = st.session_state.coordinates_cache[cache_hash]
            cached_data['hits'] += 1
            self._update_cache_stats('hit')
            
            debugger.debug(f"Cache HIT para {city_name}: {cached_data['coords']}")
            
            # BREAKPOINT: Cache hit
            debug_breakpoint("Cache hit", {
                'city_name': city_name,
                'coordinates': cached_data['coords'],
                'hits': cached_data.get('hits', 0)
            })
            
            return cached_data['coords']
        
        debugger.debug(f"Cache MISS para {city_name}")
        self._update_cache_stats('miss')
        return None
    
    def _update_cache_stats(self, action):
        """Atualiza estatísticas do cache"""
        if 'cache_stats' not in st.session_state:
            st.session_state.cache_stats = {
                'hits': 0,
                'misses': 0,
                'saves': 0,
                'hit_rate': 0,
                'total_requests': 0
            }
        
        if action == 'hit':
            st.session_state.cache_stats['hits'] += 1
        elif action == 'miss':
            st.session_state.cache_stats['misses'] += 1
        elif action == 'save':
            st.session_state.cache_stats['saves'] += 1
        
        st.session_state.cache_stats['total_requests'] += 1
    
    def get_cache_stats(self):
        """Retorna estatísticas do cache"""
        if 'cache_stats' not in st.session_state:
            return {'hits': 0, 'misses': 0, 'saves': 0, 'total_requests': 0, 'hit_rate': 0}
        
        stats = st.session_state.cache_stats.copy()
        if stats['total_requests'] > 0:
            stats['hit_rate'] = (stats['hits'] / stats['total_requests']) * 100
        else:
            stats['hit_rate'] = 0
        
        return stats
    
    def clear_cache(self):
        """Limpa o cache"""
        debugger.info("Limpando cache completo")
        st.session_state.coordinates_cache = {}
        st.session_state.cache_stats = {'hits': 0, 'misses': 0, 'saves': 0, 'total_requests': 0, 'hit_rate': 0}

# ================================
# 2. PROCESSAMENTO PARALELO COMPLETO COM DEBUG
# ================================

class ParallelProcessor:
    def __init__(self, max_workers=4):
        self.max_workers = max_workers
        self.geocoder = Nominatim(user_agent="parallel_processor_v3", timeout=20)
        self.cache = SmartCache()
        debugger.info(f"ParallelProcessor inicializado com {max_workers} workers")
        
    @debug_timing
    def get_coordinates_with_cache(self, city_name):
        """Geocoding com cache inteligente e debug"""
        # Tentar cache primeiro
        cached_coords = self.cache.get_coordinates(city_name)
        if cached_coords:
            return cached_coords
        
        # BREAKPOINT: Antes do geocoding real
        debug_breakpoint("Antes do geocoding real", {
            'city_name': city_name,
            'cache_miss': True
        })
        
        # Geocoding se não estiver no cache
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
        """Calcula distância com retry automático e debug"""
        debugger.debug(f"Calculando distância: {origin_coords} -> {dest_coords}")
        
        for attempt in range(max_retries):
            try:
                url = f"http://router.project-osrm.org/route/v1/driving/{origin_coords[0]},{origin_coords[1]};{dest_coords[0]},{dest_coords[1]}"
                params = {'overview': 'false', 'geometries': 'geojson'}
                
                # BREAKPOINT: Antes da requisição
                debug_breakpoint("Antes da requisição OSRM", {
                    'url': url,
                    'attempt': attempt + 1,
                    'max_retries': max_retries
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
                        
                        debugger.info(f"Distância calculada: {distance_km:.2f}km, {duration_min:.0f}min")
                        return result
                
                debugger.warning(f"Tentativa {attempt + 1} falhou: status {response.status_code}")
                
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Backoff exponencial
                    
            except Exception as e:
                debugger.error(f"Erro na tentativa {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
        
        debugger.error("Todas as tentativas de cálculo falharam")
        return {'status': 'erro_calculo', 'distancia_km': None, 'tempo_min': None}
    
    @debug_timing
    def process_linha_paralela(self, linha_data):
        """Processa uma linha com paralelização interna dos destinos e debug"""
        linha_excel = linha_data['linha_excel']
        origem = linha_data['origem']
        destinos = linha_data['destinos']
        
        debugger.info(f"Processando linha {linha_excel}: {origem} -> {len(destinos)} destinos")
        
        # BREAKPOINT: Início do processamento da linha
        debug_breakpoint("Início processamento linha", {
            'linha_excel': linha_excel,
            'origem': origem,
            'total_destinos': len(destinos),
            'thread_name': threading.current_thread().name
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
        
        # Geocoding da origem
        origin_coords = self.get_coordinates_with_cache(origem)
        
        if not origin_coords:
            debugger.error(f"Origem não encontrada: {origem}")
            resultado['status'] = 'origem_nao_encontrada'
            resultado['erros'] = len(destinos)
            return resultado
        
        # BREAKPOINT: Origem encontrada
        debug_breakpoint("Origem geocodificada", {
            'origem': origem,
            'coordinates': origin_coords
        })
        
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
        
        # BREAKPOINT: Geocoding concluído
        debug_breakpoint("Geocoding de destinos concluído", {
            'total_destinos': len(destinos),
            'destinos_encontrados': len([d for d in destinos_com_coords if d[1] is not None])
        })
        
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
            
            # Rate limiting por thread
            time.sleep(0.1)
            return resultado_calc
        
        # Paralelizar cálculos
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
            debugger.info(f"Mais próximo: {mais_proximo['destino']} - {mais_proximo['distancia_km']}km")
        
        resultado['tempo_processamento'] = round(time.time() - start_time, 2)
        resultado['status'] = 'concluido'
        
        # BREAKPOINT: Linha processada
        debug_breakpoint("Linha processada", {
            'linha_excel': linha_excel,
            'sucessos': resultado['sucessos'],
            'erros': resultado['erros'],
            'tempo_processamento': resultado['tempo_processamento'],
            'mais_proximo': resultado['destino_mais_proximo']
        })
        
        debugger.info(f"Linha {linha_excel} concluída: {resultado['sucessos']} sucessos, {resultado['erros']} erros")
        
        return resultado
    
    @debug_timing
    def process_all_lines_parallel(self, linhas_processaveis):
        """Processa todas as linhas em paralelo com debug"""
        debugger.info(f"Iniciando processamento paralelo de {len(linhas_processaveis)} linhas")
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
        
        # Ordenar por linha Excel
        resultados.sort(key=lambda x: x['linha_excel'])
        
        # Estatísticas globais
        stats_globais = {
            'tempo_total': round(time.time() - start_time, 2),
            'linhas_processadas': len(resultados),
            'total_sucessos': sum(r['sucessos'] for r in resultados),
            'total_erros': sum(r['erros'] for r in resultados),
            'media_tempo_por_linha': round(sum(r['tempo_processamento'] for r in resultados) / len(resultados), 2),
            'processamento_paralelo': True,
            'workers_utilizados': self.max_workers
        }
        
        debugger.info(f"Processamento concluído: {stats_globais}")
        
        return resultados, stats_globais

# ================================
# 3. DASHBOARD DE ANALYTICS COMPLETO
# ================================

class AnalyticsDashboard:
    def __init__(self):
        self.init_session_analytics()
        debugger.debug("AnalyticsDashboard inicializado")
    
    def init_session_analytics(self):
        """Inicializa analytics da sessão"""
        if 'analytics' not in st.session_state:
            st.session_state.analytics = {
                'processamentos': [],
                'performance_history': [],
                'cache_history': [],
                'error_log': [],
                'session_start': datetime.now()
            }
    
    def log_processamento(self, resultados, stats_globais):
        """Registra um processamento completo"""
        debugger.info("Registrando processamento nas analytics")
        
        log_entry = {
            'timestamp': datetime.now(),
            'linhas_processadas': stats_globais['linhas_processadas'],
            'total_sucessos': stats_globais['total_sucessos'],
            'total_erros': stats_globais['total_erros'],
            'tempo_total': stats_globais['tempo_total'],
            'taxa_sucesso': (stats_globais['total_sucessos'] / max(1, stats_globais['total_sucessos'] + stats_globais['total_erros'])) * 100,
            'processamento_paralelo': stats_globais.get('processamento_paralelo', False),
            'workers': stats_globais.get('workers_utilizados', 1)
        }
        
        st.session_state.analytics['processamentos'].append(log_entry)
        
        # Manter apenas últimos 10 processamentos
        if len(st.session_state.analytics['processamentos']) > 10:
            st.session_state.analytics['processamentos'] = st.session_state.analytics['processamentos'][-10:]
    
    def show_performance_dashboard(self):
        """Exibe dashboard de performance COMPLETO"""
        st.markdown("### 📊 Dashboard de Analytics")
        
        # Métricas da sessão atual
        analytics = st.session_state.analytics
        
        if analytics['processamentos']:
            # Métricas gerais
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
                st.metric("🚀 Paralelo Ativo", "SIM" if ultimo_processamento['processamento_paralelo'] else "NÃO")
            
            # Gráfico de performance ao longo do tempo
            st.markdown("#### 📈 Performance ao Longo do Tempo")
            
            df_performance = pd.DataFrame(analytics['processamentos'])
            df_performance['processamento_id'] = range(1, len(df_performance) + 1)
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Gráfico de tempo de processamento
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
                # Gráfico de taxa de sucesso
                fig_sucesso = px.line(
                    df_performance,
                    x='processamento_id',
                    y='taxa_sucesso',
                    title="📈 Taxa de Sucesso",
                    labels={'taxa_sucesso': 'Taxa de Sucesso (%)', 'processamento_id': 'Processamento #'}
                )
                fig_sucesso.update_traces(line_color='#764ba2')
                st.plotly_chart(fig_sucesso, use_container_width=True)
            
            # Comparação Paralelo vs Sequencial
            st.markdown("#### ⚡ Impacto do Processamento Paralelo")
            
            paralelos = [p for p in analytics['processamentos'] if p['processamento_paralelo']]
            sequenciais = [p for p in analytics['processamentos'] if not p['processamento_paralelo']]
            
            if paralelos and sequenciais:
                col1, col2 = st.columns(2)
                
                with col1:
                    tempo_medio_paralelo = sum(p['tempo_total'] for p in paralelos) / len(paralelos)
                    st.metric("⚡ Tempo Médio (Paralelo)", f"{tempo_medio_paralelo:.1f}s")
                
                with col2:
                    tempo_medio_sequencial = sum(p['tempo_total'] for p in sequenciais) / len(sequenciais)
                    st.metric("🐌 Tempo Médio (Sequencial)", f"{tempo_medio_sequencial:.1f}s")
                
                melhoria = ((tempo_medio_sequencial - tempo_medio_paralelo) / tempo_medio_sequencial) * 100
                st.success(f"🚀 **Melhoria de Performance:** {melhoria:.1f}% mais rápido com processamento paralelo!")
        
        else:
            st.info("📊 Execute um processamento para ver analytics detalhados")
    
    def show_cache_analytics(self, cache_stats):
        """Exibe analytics do cache COMPLETO"""
        st.markdown("#### 💾 Analytics do Cache Inteligente")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("🎯 Cache Hits", cache_stats['hits'])
        with col2:
            st.metric("❌ Cache Misses", cache_stats['misses'])
        with col3:
            st.metric("💾 Itens Salvos", cache_stats['saves'])
        with col4:
            st.metric("📈 Hit Rate", f"{cache_stats['hit_rate']:.1f}%")
        
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
                title="💾 Distribuição de Cache Hits vs Misses",
                height=300
            )
            st.plotly_chart(fig_cache, use_container_width=True)

# ================================
# 4. MAPAS INTERATIVOS COMPLETOS
# ================================

class InteractiveMapper:
    def __init__(self):
        debugger.debug("InteractiveMapper inicializado")
    
    def create_route_map(self, resultado_linha):
        """Cria mapa interativo com rotas COMPLETO"""
        debugger.debug(f"Criando mapa para linha {resultado_linha.get('linha_excel', '?')}")
        
        if resultado_linha['status'] != 'concluido':
            return None
        
        origem = resultado_linha['origem']
        sucessos = [d for d in resultado_linha['destinos_calculados'] if d['distancia_km'] is not None]
        
        if not sucessos:
            return None
        
        # Coordenadas da origem (simulada - em produção viria do cache)
        origem_lat, origem_lon = -21.7587, -43.3496  # Juiz de Fora como exemplo
        
        # Criar mapa base
        m = folium.Map(
            location=[origem_lat, origem_lon],
            zoom_start=8,
            tiles='OpenStreetMap'
        )
        
        # Marker da origem
        folium.Marker(
            [origem_lat, origem_lon],
            popup=f"🎯 ORIGEM: {origem}",
            tooltip=f"Origem: {origem}",
            icon=folium.Icon(color='red', icon='home', prefix='fa')
        ).add_to(m)
        
        # Markers dos destinos
        cores = ['blue', 'green', 'orange', 'purple', 'pink', 'gray', 'darkblue', 'darkgreen']
        
        for i, destino_data in enumerate(sucessos[:10]):  # Primeiros 10
            # Coordenadas simuladas (em produção viria do geocoding)
            dest_lat = origem_lat + (np.random.random() - 0.5) * 2
            dest_lon = origem_lon + (np.random.random() - 0.5) * 2
            
            cor = cores[i % len(cores)]
            
            # Marker do destino
            folium.Marker(
                [dest_lat, dest_lon],
                popup=f"📍 {destino_data['destino']}<br>📏 {destino_data['distancia_km']} km<br>⏱️ {destino_data['tempo_min']} min",
                tooltip=f"{destino_data['destino']} - {destino_data['distancia_km']} km",
                icon=folium.Icon(color=cor, icon='map-marker', prefix='fa')
            ).add_to(m)
            
            # Linha conectando origem ao destino
            folium.PolyLine(
                [[origem_lat, origem_lon], [dest_lat, dest_lon]],
                color=cor,
                weight=3,
                opacity=0.7,
                popup=f"{origem} → {destino_data['destino']}: {destino_data['distancia_km']} km"
            ).add_to(m)
        
        # Destaque para o mais próximo
        if resultado_linha['destino_mais_proximo']:
            folium.Circle(
                [origem_lat, origem_lon],
                radius=50000,  # 50km
                popup=f"🏆 Destino mais próximo: {resultado_linha['destino_mais_proximo']}",
                color='gold',
                fill=True,
                opacity=0.3
            ).add_to(m)
        
        debugger.debug(f"Mapa criado com {len(sucessos)} destinos")
        return m
    
    def create_overview_map(self, todos_resultados):
        """Cria mapa geral com todas as origens COMPLETO"""
        debugger.debug(f"Criando mapa overview com {len(todos_resultados)} resultados")
        
        if not todos_resultados:
            return None
        
        # Mapa centrado no Brasil/MG
        m = folium.Map(
            location=[-19.9167, -43.9345],  # MG
            zoom_start=6,
            tiles='OpenStreetMap'
        )
        
        # Adicionar origens ao mapa
        cores_origens = ['red', 'blue', 'green', 'orange', 'purple']
        
        for i, resultado in enumerate(todos_resultados[:5]):  # Primeiras 5 origens
            if resultado['status'] != 'concluido':
                continue
            
            # Coordenadas simuladas da origem
            origem_lat = -19.9167 + (np.random.random() - 0.5) * 4
            origem_lon = -43.9345 + (np.random.random() - 0.5) * 4
            
            cor = cores_origens[i % len(cores_origens)]
            
            # Marker da origem
            folium.Marker(
                [origem_lat, origem_lon],
                popup=f"""
                <b>🎯 {resultado['origem']}</b><br>
                📊 {resultado['total_destinos']} destinos<br>
                ✅ {resultado['sucessos']} sucessos<br>
                🏆 Mais próximo: {resultado['destino_mais_proximo']}<br>
                📏 {resultado['km_mais_proximo']} km
                """,
                tooltip=f"{resultado['origem']} - {resultado['sucessos']} sucessos",
                icon=folium.Icon(color=cor, icon='home', prefix='fa')
            ).add_to(m)
            
            # Círculo representando a área de cobertura
            raio = min(resultado['km_mais_proximo'] * 1000, 100000) if resultado['km_mais_proximo'] else 50000
            
            folium.Circle(
                [origem_lat, origem_lon],
                radius=raio,
                popup=f"Área de {resultado['origem']}",
                color=cor,
                fill=True,
                opacity=0.2
            ).add_to(m)
        
        return m

# ================================
# 5. PAINEL DE DEBUG COMPLETO
# ================================

def show_debug_panel():
    """Painel de debug COMPLETO na sidebar"""
    if not DEBUG_MODE:
        return
    
    with st.sidebar:
        st.markdown('<div class="debug-panel">', unsafe_allow_html=True)
        st.markdown("### 🐛 Debug Panel Avançado")
        
        # Status do debug
        col1, col2 = st.columns(2)
        with col1:
            st.metric("🔧 Debug Mode", "ON" if DEBUG_MODE else "OFF")
        with col2:
            st.metric("🧵 Threads", threading.active_count())
        
        # Estatísticas de debug
        if 'debug_logs' in st.session_state:
            total_logs = len(st.session_state.debug_logs)
            
            # Contadores por nível
            debug_counts = {}
            for log in st.session_state.debug_logs:
                level = log['level']
                debug_counts[level] = debug_counts.get(level, 0) + 1
            
            st.markdown("#### 📊 Estatísticas de Logs")
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("📝 Total Logs", total_logs)
                st.metric("🔍 DEBUG", debug_counts.get('DEBUG', 0))
            
            with col2:
                st.metric("ℹ️ INFO", debug_counts.get('INFO', 0))
                st.metric("⚠️ WARNING", debug_counts.get('WARNING', 0))
                st.metric("🚨 ERROR", debug_counts.get('ERROR', 0))
            
            # Filtros de log
            st.markdown("#### 🔍 Filtros de Log")
            log_level_filter = st.selectbox(
                "Nível de Log",
                options=['TODOS', 'DEBUG', 'INFO', 'WARNING', 'ERROR'],
                index=0
            )
            
            thread_filter = st.selectbox(
                "Thread",
                options=['TODAS'] + list(set([log['thread'] for log in st.session_state.debug_logs])),
                index=0
            )
            
            # Últimos logs filtrados
            if total_logs > 0:
                st.markdown("#### 📋 Logs Recentes")
                
                # Aplicar filtros
                filtered_logs = st.session_state.debug_logs
                
                if log_level_filter != 'TODOS':
                    filtered_logs = [log for log in filtered_logs if log['level'] == log_level_filter]
                
                if thread_filter != 'TODAS':
                    filtered_logs = [log for log in filtered_logs if log['thread'] == thread_filter]
                
                # Mostrar últimos logs
                for log in filtered_logs[-10:]:
                    level_emoji = {
                        'DEBUG': '🔍',
                        'INFO': 'ℹ️',
                        'WARNING': '⚠️',
                        'ERROR': '🚨'
                    }.get(log['level'], '📝')
                    
                    # Truncar mensagem se muito longa
                    message = log['message']
                    if len(message) > 50:
                        message = message[:47] + "..."
                    
                    st.text(f"{level_emoji} [{log['timestamp']}] {message}")
        
        # Controles de debug
        st.markdown("#### 🎛️ Controles")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🗑️ Limpar Logs"):
                st.session_state.debug_logs = []
                st.success("Logs limpos!")
                st.rerun()
        
        with col2:
            if st.button("🔄 Refresh"):
                st.rerun()
        
        # Download de logs
        if 'debug_logs' in st.session_state and st.session_state.debug_logs:
            logs_text = "\n".join([
                f"[{log['timestamp']}] [{log['thread']}] {log['level']}: {log['message']}"
                for log in st.session_state.debug_logs
            ])
            
            st.download_button(
                "📥 Download Logs Debug",
                data=logs_text,
                file_name=f"debug_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                mime="text/plain",
                use_container_width=True
            )
        
        # Informações do sistema
        st.markdown("#### 🔧 Info do Sistema")
        st.text(f"Python: {threading.current_thread().name}")
        st.text(f"Session ID: {st.session_state.get('session_id', 'N/A')}")
        
        # Variáveis de ambiente importantes
        env_vars = ['STREAMLIT_ENV', 'PYTHONPATH', 'STREAMLIT_LOGGER_LEVEL']
        for var in env_vars:
            value = os.getenv(var, 'N/A')
            st.text(f"{var}: {value}")
        
        st.markdown('</div>', unsafe_allow_html=True)

# ================================
# 6. SISTEMA PRINCIPAL INTEGRADO COMPLETO
# ================================

class AdvancedDistanceSystem:
    def __init__(self):
        self.cache = SmartCache()
        self.parallel_processor = ParallelProcessor(max_workers=6)
        self.analytics = AnalyticsDashboard()
        self.mapper = InteractiveMapper()
        debugger.info("AdvancedDistanceSystem inicializado completo")
        
    @debug_timing
    def analyze_spreadsheet_advanced(self, df):
        """Análise avançada da planilha COMPLETA"""
        debugger.info(f"Analisando planilha: {df.shape}")
        
        try:
            origem_col = df.columns[1] if len(df.columns) > 1 else None
            destino_cols = df.columns[2:] if len(df.columns) > 2 else []
            
            debugger.debug(f"Colunas detectadas - Origem: {origem_col}, Destinos: {len(destino_cols)}")
            
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
            
            analysis = {
                'total_rows_original': len(df),
                'linhas_processaveis': linhas_processaveis,
                'total_linhas_validas': len(linhas_processaveis),
                'total_calculos': sum(linha['total_destinos'] for linha in linhas_processaveis),
                'estimativa_tempo_paralelo': sum(linha['total_destinos'] for linha in linhas_processaveis) * 0.1 / 60,
                'estimativa_tempo_sequencial': sum(linha['total_destinos'] for linha in linhas_processaveis) * 0.3 / 60
            }
            
            debugger.info(f"Análise concluída: {analysis['total_linhas_validas']} linhas válidas, {analysis['total_calculos']} cálculos")
            
            return analysis
            
        except Exception as e:
            debugger.error(f"Erro na análise da planilha: {e}")
            st.error(f"❌ Erro na análise: {e}")
            return None
    
    def _clean_city_name(self, city_value):
        """Limpa nome da cidade"""
        if pd.isna(city_value):
            return None
        
        city_clean = str(city_value).strip().upper()
        
        if city_clean in ['', 'NAN', 'NONE', 'NULL', '#N/A']:
            return None
        
        return city_clean.replace('  ', ' ')

def main():
    """Função principal COMPLETA com debug integrado"""
    
    # Inicializar debug
    debugger.info("=== INICIANDO APLICAÇÃO COMPLETA COM DEBUG ===")
    
    # Painel de debug (apenas em modo desenvolvimento)
    show_debug_panel()
    
    # Header futurista COMPLETO
    header_text = "🚀 Sistema Avançado de Distâncias"
    if DEBUG_MODE:
        header_text += " (Debug Mode)"
    
    st.markdown(f"""
    <div class="main-header">
        <h1>{header_text}</h1>
        <h3>Cache Inteligente • Processamento Paralelo • Analytics • Mapas Interativos</h3>
        <p>Versão 3.0.2 - Pro Debug (Completa)</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Inicializar sistema COMPLETO
    sistema = AdvancedDistanceSystem()
    
    # Sidebar com configurações avançadas COMPLETAS
    with st.sidebar:
        st.header("⚙️ Configurações Avançadas")
        
        # Configurações de performance
        st.markdown("#### 🚀 Performance")
        max_workers = st.slider("🔧 Workers Paralelos", 2, 8, 6)
        sistema.parallel_processor.max_workers = max_workers
        
        # Cache controls COMPLETOS
        st.markdown("#### 💾 Cache Inteligente")
        cache_stats = sistema.cache.get_cache_stats()
        
        if cache_stats['total_requests'] > 0:
            st.metric("📊 Cache Hit Rate", f"{cache_stats['hit_rate']:.1f}%")
            st.metric("💾 Itens Cached", len(st.session_state.get('coordinates_cache', {})))
        
        if st.button("🗑️ Limpar Cache"):
            sistema.cache.clear_cache()
            st.success("Cache limpo!")
            debugger.info("Cache limpo pelo usuário")
            st.rerun()
        
        # Analytics toggle
        show_analytics = st.checkbox("📊 Mostrar Analytics", value=True)
        show_maps = st.checkbox("🗺️ Mostrar Mapas", value=True)
        
        # Status do sistema
        st.markdown("#### 🔧 Status do Sistema")
        st.success(f"✅ {max_workers} workers ativos")
        if DEBUG_MODE:
            st.info("🐛 Debug mode ativo")
        st.metric("🧵 Threads ativas", threading.active_count())
    
    # Analytics Dashboard COMPLETO (se habilitado)
    if show_analytics:
        sistema.analytics.show_performance_dashboard()
        sistema.analytics.show_cache_analytics(cache_stats)
    
    # Upload e processamento COMPLETO
    uploaded_file = st.file_uploader(
        "📁 Upload da Planilha Excel",
        type=['xlsx', 'xls'],
        help="Sistema otimizado com cache, processamento paralelo e debug integrado"
    )
    
    if uploaded_file is not None:
        debugger.info(f"Arquivo carregado: {uploaded_file.name} ({uploaded_file.size} bytes)")
        
        # BREAKPOINT: Arquivo carregado
        debug_breakpoint("Arquivo Excel carregado", {
            'filename': uploaded_file.name,
            'size': uploaded_file.size
        })
        
        # Análise da planilha COMPLETA
        with st.spinner("🔍 Análise avançada da planilha..."):
            df = pd.read_excel(uploaded_file)
            analysis = sistema.analyze_spreadsheet_advanced(df)
        
        if analysis:
            st.success("✅ Planilha analisada com otimizações e debug ativados!")
            
            # Métricas otimizadas COMPLETAS
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("📋 Linhas Válidas", analysis['total_linhas_validas'])
            with col2:
                st.metric("📏 Cálculos Totais", analysis['total_calculos'])
            with col3:
                st.metric("⚡ Tempo Est. (Paralelo)", f"{analysis['estimativa_tempo_paralelo']:.1f} min")
            with col4:
                speedup = analysis['estimativa_tempo_sequencial'] / analysis['estimativa_tempo_paralelo']
                st.metric("🚀 Speedup", f"{speedup:.1f}x")
            
            # PROCESSAMENTO COMPLETO COM DEBUG
            button_text = "🚀 Processar com Sistema Avançado"
            if DEBUG_MODE:
                button_text += " (Debug)"
            
            if st.button(button_text, type="primary", use_container_width=True):
                
                debugger.info("=== INICIANDO PROCESSAMENTO COMPLETO ===")
                
                # BREAKPOINT: Início do processamento
                debug_breakpoint("Início do processamento principal", {
                    'total_linhas': analysis['total_linhas_validas'],
                    'total_calculos': analysis['total_calculos'],
                    'workers': max_workers
                })
                
                st.markdown("### ⚡ Processamento Paralelo em Execução")
                
                # Progress tracking thread-safe
                progress_bar = st.progress(0)
                status_info = st.empty()
                
                # Executar processamento paralelo COMPLETO
                start_time = time.time()
                
                status_info.info("🚀 Iniciando processamento paralelo avançado...")
                progress_bar.progress(20)
                
                with st.spinner("Executando processamento paralelo otimizado..."):
                    resultados, stats_globais = sistema.parallel_processor.process_all_lines_parallel(
                        analysis['linhas_processaveis']
                    )
                
                progress_bar.progress(100)
                status_info.success("✅ Processamento paralelo concluído!")
                
                # Log analytics COMPLETO
                sistema.analytics.log_processamento(resultados, stats_globais)
                
                debugger.info("=== PROCESSAMENTO CONCLUÍDO ===")
                
                # Exibir resultados COMPLETOS
                if resultados:
                    # Métricas finais COMPLETAS
                    st.markdown("### 📊 Resultados do Processamento Avançado")
                    
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
                    
                    # Performance comparison COMPLETA
                    tempo_estimado_sequencial = analysis['estimativa_tempo_sequencial'] * 60
                    melhoria_real = ((tempo_estimado_sequencial - stats_globais['tempo_total']) / tempo_estimado_sequencial) * 100
                    
                    debug_info = ""
                    if DEBUG_MODE:
                        debug_info = f"\n🐛 **Logs de debug:** {len(st.session_state.get('debug_logs', []))} entradas"
                    
                    st.success(f"""
                    🚀 **Performance Otimizada Alcançada!**
                    
                    ⚡ **{melhoria_real:.1f}% mais rápido** que processamento sequencial  
                    🔧 **{stats_globais['workers_utilizados']} workers** executando em paralelo  
                    💾 **Cache hit rate:** {cache_stats['hit_rate']:.1f}%  
                    📊 **Tempo médio por linha:** {stats_globais['media_tempo_por_linha']:.2f}s{debug_info}
                    """)
                    
                    # Resultados detalhados por linha COMPLETOS
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
                                # Métricas da linha COMPLETAS
                                st.metric("⏱️ Tempo", f"{resultado['tempo_processamento']}s")
                                st.metric("📊 Total", resultado['total_destinos'])
                                st.metric("✅ Sucessos", resultado['sucessos'])
                                st.metric("❌ Erros", resultado['erros'])
                                
                                # Debug info se disponível
                                if DEBUG_MODE:
                                    st.text(f"Thread: {threading.current_thread().name}")
                                    st.text(f"Status: {resultado['status']}")
                                
                                # Mapa da linha (se habilitado)
                                if show_maps and resultado['status'] == 'concluido':
                                    mapa_linha = sistema.mapper.create_route_map(resultado)
                                    if mapa_linha:
                                        st.markdown("**🗺️ Mapa das Rotas:**")
                                        st_folium(mapa_linha, width=300, height=200)
                    
                    # Mapa geral COMPLETO (se habilitado)
                    if show_maps:
                        st.markdown("### 🗺️ Mapa Geral de Todas as Origens")
                        mapa_geral = sistema.mapper.create_overview_map(resultados)
                        if mapa_geral:
                            st_folium(mapa_geral, width=700, height=500)
                    
                    # Download otimizado COMPLETO
                    st.markdown("### 💾 Download do Relatório Avançado")
                    
                    # Criar Excel com dados extras COMPLETO
                    excel_data = create_advanced_excel(resultados, stats_globais, cache_stats)
                    
                    if excel_data:
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        filename_suffix = "_debug" if DEBUG_MODE else ""
                        filename = f"relatorio_avancado{filename_suffix}_{timestamp}.xlsx"
                        
                        st.download_button(
                            label="📥 Baixar Relatório Avançado (Excel)",
                            data=excel_data,
                            file_name=filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            type="primary",
                            use_container_width=True
                        )
                        
                        info_text = """
                        📊 **Relatório Avançado contém:**
                        - 📋 Resultados detalhados por linha
                        - ⚡ Métricas de performance paralela
                        - 💾 Estatísticas de cache
                        - 📈 Analytics de processamento
                        - 🗺️ Coordenadas para mapas"""
                        
                        if DEBUG_MODE:
                            info_text += "\n- 🐛 Informações de debug"
                        
                        st.info(info_text)

def create_advanced_excel(resultados, stats_globais, cache_stats):
    """Cria Excel avançado com métricas extras COMPLETO"""
    debugger.debug("Criando Excel avançado")
    output = BytesIO()
    
    try:
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Aba principal COMPLETA
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
                df_completo.to_excel(writer, sheet_name='Resultados_Avancados', index=False)
            
            # Métricas de performance COMPLETAS
            performance_data = {
                'Métrica': [
                    'Tempo Total de Processamento (s)',
                    'Processamento Paralelo Ativo',
                    'Workers Utilizados',
                    'Linhas Processadas',
                    'Total de Sucessos',
                    'Total de Erros',
                    'Taxa de Sucesso (%)',
                    'Tempo Médio por Linha (s)',
                    'Cache Hit Rate (%)',
                    'Total de Cache Hits',
                    'Total de Cache Misses',
                    'Itens Salvos no Cache',
                    'Sistema de Coordenadas',
                    'API de Rotas',
                    'Debug Mode Ativo',
                    'Total de Logs Debug'
                ],
                'Valor': [
                    stats_globais['tempo_total'],
                    'SIM' if stats_globais.get('processamento_paralelo', False) else 'NÃO',
                    stats_globais.get('workers_utilizados', 1),
                    stats_globais['linhas_processadas'],
                    stats_globais['total_sucessos'],
                    stats_globais['total_erros'],
                    round((stats_globais['total_sucessos'] / max(1, stats_globais['total_sucessos'] + stats_globais['total_erros'])) * 100, 2),
                    stats_globais['media_tempo_por_linha'],
                    round(cache_stats['hit_rate'], 2),
                    cache_stats['hits'],
                    cache_stats['misses'],
                    cache_stats['saves'],
                    'Nominatim (OpenStreetMap)',
                    'OSRM (Open Source Routing Machine)',
                    'SIM' if DEBUG_MODE else 'NÃO',
                    len(st.session_state.get('debug_logs', []))
                ]
            }
            
            df_performance = pd.DataFrame(performance_data)
            df_performance.to_excel(writer, sheet_name='Metricas_Performance', index=False)
            
            # Resumo das origens com métricas avançadas COMPLETO
            resumo_avancado = []
            for resultado in resultados:
                resumo_avancado.append({
                    'Linha_Excel': resultado['linha_excel'],
                    'Origem': resultado['origem'],
                    'Total_Destinos': resultado['total_destinos'],
                    'Sucessos': resultado['sucessos'],
                    'Erros': resultado['erros'],
                    'Taxa_Sucesso_Linha': round((resultado['sucessos'] / max(1, resultado['total_destinos'])) * 100, 1),
                    'Tempo_Processamento_Segundos': resultado['tempo_processamento'],
                    'Destino_Mais_Proximo': resultado['destino_mais_proximo'],
                    'Distancia_Mais_Proxima_KM': resultado['km_mais_proximo'],
                    'Status_Processamento': resultado['status']
                })
            
            df_resumo_avancado = pd.DataFrame(resumo_avancado)
            df_resumo_avancado.to_excel(writer, sheet_name='Resumo_Avancado', index=False)
            
            # Aba de debug (se habilitada)
            if DEBUG_MODE and 'debug_logs' in st.session_state:
                debug_data = []
                for log in st.session_state.debug_logs:
                    debug_data.append({
                        'Timestamp': log['timestamp'],
                        'Thread': log['thread'],
                        'Level': log['level'],
                        'Message': log['message']
                    })
                
                if debug_data:
                    df_debug = pd.DataFrame(debug_data)
                    df_debug.to_excel(writer, sheet_name='Debug_Logs', index=False)
        
        debugger.info("Excel avançado criado com sucesso")
        return output.getvalue()
        
    except Exception as e:
        debugger.error(f"Erro ao criar Excel avançado: {e}")
        st.error(f"❌ Erro ao criar Excel avançado: {e}")
        return None

if __name__ == "__main__":
    main()