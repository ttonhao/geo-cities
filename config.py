 # type: ignore
"""
⚙️ CONFIGURAÇÕES AVANÇADAS DO SISTEMA
===================================

Arquivo de configuração para otimizar performance
baseado no hardware e necessidades específicas.

Versão: 3.0.0
"""

import os
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class CacheConfig:
    """Configurações do cache inteligente"""
    ttl_hours: int = 24                    # Time-to-live em horas
    max_entries: int = 10000               # Máximo de entradas no cache
    enable_persistence: bool = True         # Cache persistente entre sessões
    auto_cleanup: bool = True              # Limpeza automática de entradas antigas
    hit_rate_target: float = 0.8           # Meta de hit rate (80%)

@dataclass
class ParallelConfig:
    """Configurações de processamento paralelo"""
    max_workers: int = 6                   # Workers padrão
    min_workers: int = 1                   # Mínimo de workers
    auto_scaling: bool = True              # Auto-scale baseado no dataset
    rate_limit_per_thread: float = 0.1     # Delay entre requisições por thread
    timeout_seconds: int = 25              # Timeout por requisição
    retry_attempts: int = 3                # Tentativas de retry
    backoff_factor: float = 2.0            # Fator de backoff exponencial

@dataclass
class AnalyticsConfig:
    """Configurações de analytics"""
    enable_real_time: bool = True          # Analytics em tempo real
    history_limit: int = 50                # Limite de histórico de processamentos
    auto_export: bool = True               # Export automático de métricas
    performance_alerts: bool = True        # Alertas de performance
    benchmark_mode: bool = False           # Modo benchmark detalhado

@dataclass
class MapConfig:
    """Configurações de mapas"""
    enable_interactive: bool = True        # Mapas interativos
    default_zoom: int = 8                  # Zoom padrão
    max_markers: int = 50                  # Máximo de markers por mapa
    route_colors: List[str] = None         # Cores das rotas
    center_lat: float = -19.9167           # Centro padrão (MG)
    center_lon: float = -43.9345           # Centro padrão (MG)
    
    def __post_init__(self):
        if self.route_colors is None:
            self.route_colors = [
                '#FF6B35', '#F7931E', '#FFD23F', '#06FFA5',
                '#118AB2', '#073B4C', '#A663CC', '#FF006E'
            ]

@dataclass
class GeocodingConfig:
    """Configurações de geocoding"""
    user_agent: str = "advanced_distance_system_v3"
    timeout_seconds: int = 20              # Timeout para geocoding
    fallback_strategies: List[str] = None  # Estratégias de fallback
    brazil_priority: bool = True           # Priorizar resultados do Brasil
    mg_priority: bool = True               # Priorizar MG quando possível
    
    def __post_init__(self):
        if self.fallback_strategies is None:
            self.fallback_strategies = [
                "{city}, MG, Brasil",
                "{city}, Brasil", 
                "{city}"
            ]

@dataclass
class RoutingConfig:
    """Configurações de cálculo de rotas"""
    api_base_url: str = "http://router.project-osrm.org"
    route_profile: str = "driving"         # driving, walking, cycling
    overview: str = "false"                # Detalhes da geometria
    alternatives: bool = False             # Rotas alternativas
    steps: bool = False                    # Instruções passo-a-passo
    annotations: bool = False              # Anotações detalhadas

class SystemConfig:
    """Configuração geral do sistema"""
    
    def __init__(self):
        self.cache = CacheConfig()
        self.parallel = ParallelConfig()
        self.analytics = AnalyticsConfig()
        self.maps = MapConfig()
        self.geocoding = GeocodingConfig()
        self.routing = RoutingConfig()
        
        # Auto-detecção de hardware
        self._detect_hardware()
        
        # Aplicar otimizações baseadas no ambiente
        self._apply_optimizations()
    
    def _detect_hardware(self):
        """Detecta capacidades do hardware"""
        try:
            import psutil
            
            # CPU cores
            cpu_count = psutil.cpu_count(logical=True)
            
            # Memória disponível
            memory_gb = psutil.virtual_memory().total / (1024**3)
            
            # Ajustar workers baseado no hardware
            if cpu_count >= 8:
                self.parallel.max_workers = min(8, cpu_count - 2)
            elif cpu_count >= 4:
                self.parallel.max_workers = min(6, cpu_count - 1)
            else:
                self.parallel.max_workers = max(2, cpu_count)
            
            # Ajustar cache baseado na memória
            if memory_gb >= 16:
                self.cache.max_entries = 15000
            elif memory_gb >= 8:
                self.cache.max_entries = 10000
            else:
                self.cache.max_entries = 5000
                
        except ImportError:
            # Psutil não disponível, usar defaults
            pass
    
    def _apply_optimizations(self):
        """Aplica otimizações baseadas no ambiente"""
        
        # Otimizações para produção
        if os.getenv('STREAMLIT_ENV') == 'production':
            self.parallel.rate_limit_per_thread = 0.2  # Mais conservador
            self.cache.ttl_hours = 48                  # Cache mais longo
            self.analytics.history_limit = 100        # Mais histórico
        
        # Otimizações para desenvolvimento
        elif os.getenv('STREAMLIT_ENV') == 'development':
            self.parallel.rate_limit_per_thread = 0.05  # Mais agressivo
            self.cache.ttl_hours = 1                    # Cache curto para testes
            self.analytics.benchmark_mode = True        # Benchmark detalhado
    
    def get_optimized_workers(self, dataset_size: int) -> int:
        """Calcula número ótimo de workers para um dataset"""
        if dataset_size <= 10:
            return min(2, self.parallel.max_workers)
        elif dataset_size <= 50:
            return min(4, self.parallel.max_workers)
        else:
            return self.parallel.max_workers
    
    def get_rate_limit(self, workers: int) -> float:
        """Calcula rate limit ótimo baseado no número de workers"""
        base_rate = self.parallel.rate_limit_per_thread
        
        # Ajustar rate limit baseado no número de workers
        if workers >= 6:
            return base_rate * 1.5  # Mais conservador com muitos workers
        elif workers <= 2:
            return base_rate * 0.5  # Mais agressivo com poucos workers
        else:
            return base_rate
    
    def export_config(self) -> Dict:
        """Exporta configuração atual"""
        return {
            'cache': {
                'ttl_hours': self.cache.ttl_hours,
                'max_entries': self.cache.max_entries,
                'enable_persistence': self.cache.enable_persistence
            },
            'parallel': {
                'max_workers': self.parallel.max_workers,
                'rate_limit_per_thread': self.parallel.rate_limit_per_thread,
                'timeout_seconds': self.parallel.timeout_seconds
            },
            'analytics': {
                'enable_real_time': self.analytics.enable_real_time,
                'history_limit': self.analytics.history_limit
            },
            'maps': {
                'enable_interactive': self.maps.enable_interactive,
                'max_markers': self.maps.max_markers
            }
        }

# Configuração global (singleton)
CONFIG = SystemConfig()

def get_config() -> SystemConfig:
    """Retorna configuração global"""
    return CONFIG

def optimize_for_dataset(num_lines: int, total_calculations: int) -> Dict:
    """Otimiza configurações para um dataset específico"""
    config = get_config()
    
    # Calcular workers ótimos
    optimal_workers = config.get_optimized_workers(num_lines)
    
    # Calcular rate limit ótimo
    optimal_rate_limit = config.get_rate_limit(optimal_workers)
    
    # Estimar tempo de processamento
    time_per_calc = 0.4  # segundos base por cálculo
    estimated_time = (total_calculations * time_per_calc) / optimal_workers
    
    return {
        'workers': optimal_workers,
        'rate_limit': optimal_rate_limit,
        'estimated_time_minutes': estimated_time / 60,
        'cache_efficiency_expected': min(0.9, num_lines / 100),  # Eficiência esperada
        'memory_usage_mb': total_calculations * 0.1,  # Estimativa de uso de memória
        'recommendations': _generate_recommendations(num_lines, total_calculations)
    }

def _generate_recommendations(num_lines: int, total_calculations: int) -> List[str]:
    """Gera recomendações baseadas no dataset"""
    recommendations = []
    
    if num_lines > 100:
        recommendations.append("🚀 Dataset grande detectado: usar máximo de workers")
        recommendations.append("💾 Ativar cache persistente para sessões futuras")
    
    if total_calculations > 1000:
        recommendations.append("⏱️ Processamento longo esperado: considere executar em horários de menor rede")
        recommendations.append("📊 Ativar benchmark mode para análise detalhada")
    
    if num_lines < 10:
        recommendations.append("⚡ Dataset pequeno: usar poucos workers para evitar overhead")
        recommendations.append("🎯 Considere agrupar com outros processamentos")
    
    return recommendations

# Configurações específicas por ambiente
PRODUCTION_CONFIG = {
    'cache_ttl_hours': 48,
    'max_workers': 4,
    'rate_limit': 0.2,
    'enable_analytics': True,
    'enable_maps': True
}

DEVELOPMENT_CONFIG = {
    'cache_ttl_hours': 1,
    'max_workers': 8,
    'rate_limit': 0.05,
    'enable_analytics': True,
    'enable_maps': True,
    'benchmark_mode': True
}

TESTING_CONFIG = {
    'cache_ttl_hours': 0.1,
    'max_workers': 2,
    'rate_limit': 0.01,
    'enable_analytics': False,
    'enable_maps': False
}

def load_environment_config(env: str = 'production'):
    """Carrega configuração específica do ambiente"""
    global CONFIG
    
    if env == 'production':
        config_dict = PRODUCTION_CONFIG
    elif env == 'development':
        config_dict = DEVELOPMENT_CONFIG
    elif env == 'testing':
        config_dict = TESTING_CONFIG
    else:
        return CONFIG
    
    # Aplicar configurações
    for key, value in config_dict.items():
        if hasattr(CONFIG.parallel, key):
            setattr(CONFIG.parallel, key, value)
        elif hasattr(CONFIG.cache, key.replace('cache_', '')):
            setattr(CONFIG.cache, key.replace('cache_', ''), value)
        elif hasattr(CONFIG.analytics, key.replace('enable_', '')):
            setattr(CONFIG.analytics, key.replace('enable_', ''), value)
    
    return CONFIG

if __name__ == "__main__":
    # Demo das configurações
    config = get_config()
    
    print("⚙️ CONFIGURAÇÕES DO SISTEMA AVANÇADO")
    print("=" * 50)
    
    print(f"🚀 Processamento Paralelo:")
    print(f"   Workers máximos: {config.parallel.max_workers}")
    print(f"   Rate limit: {config.parallel.rate_limit_per_thread}s")
    print(f"   Timeout: {config.parallel.timeout_seconds}s")
    
    print(f"\n💾 Cache Inteligente:")
    print(f"   TTL: {config.cache.ttl_hours} horas")
    print(f"   Max entradas: {config.cache.max_entries}")
    print(f"   Persistente: {config.cache.enable_persistence}")
    
    print(f"\n📊 Analytics:")
    print(f"   Tempo real: {config.analytics.enable_real_time}")
    print(f"   Histórico: {config.analytics.history_limit}")
    
    print(f"\n🗺️ Mapas:")
    print(f"   Interativos: {config.maps.enable_interactive}")
    print(f"   Max markers: {config.maps.max_markers}")
    
    # Teste de otimização
    print(f"\n🎯 EXEMPLO DE OTIMIZAÇÃO:")
    print("=" * 50)
    
    optimization = optimize_for_dataset(num_lines=25, total_calculations=500)
    
    print(f"Dataset: 25 linhas, 500 cálculos")
    print(f"Workers recomendados: {optimization['workers']}")
    print(f"Rate limit ótimo: {optimization['rate_limit']}s")
    print(f"Tempo estimado: {optimization['estimated_time_minutes']:.1f} min")
    print(f"Eficiência cache esperada: {optimization['cache_efficiency_expected']*100:.1f}%")
    
    print(f"\n💡 Recomendações:")
    for rec in optimization['recommendations']:
        print(f"   {rec}")