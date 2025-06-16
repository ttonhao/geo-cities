import requests
import pandas as pd
import time
from geopy.geocoders import Nominatim
import json

class DistanceCalculatorCustom:
    def __init__(self):
        self.geocoder = Nominatim(user_agent="mg_distance_calculator_custom")
        
    def get_coordinates(self, city_name, state="MG"):
        """
        Obtém coordenadas lat/lng de uma cidade
        """
        try:
            # Tenta primeiro com estado específico
            location = self.geocoder.geocode(f"{city_name}, {state}, Brasil")
            if location:
                return [location.longitude, location.latitude]
            
            # Se não encontrar, tenta sem estado
            location = self.geocoder.geocode(f"{city_name}, Brasil")
            if location:
                return [location.longitude, location.latitude]
            
            print(f"⚠️ Coordenadas não encontradas para: {city_name}")
            return None
            
        except Exception as e:
            print(f"❌ Erro ao buscar coordenadas de {city_name}: {e}")
            return None
    
    def calculate_route_distance(self, origin_coords, dest_coords):
        """
        Calcula distância rodoviária usando OSRM (gratuito)
        """
        try:
            url = f"http://router.project-osrm.org/route/v1/driving/{origin_coords[0]},{origin_coords[1]};{dest_coords[0]},{dest_coords[1]}"
            params = {
                'overview': 'false',
                'geometries': 'geojson'
            }
            
            response = requests.get(url, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('routes'):
                    route = data['routes'][0]
                    distance_km = route['distance'] / 1000
                    duration_min = route['duration'] / 60
                    
                    return {
                        'distancia_km': round(distance_km, 2),
                        'tempo_minutos': round(duration_min, 0),
                        'status': 'sucesso'
                    }
            
            return {'status': 'erro_rota', 'distancia_km': None, 'tempo_minutos': None}
            
        except Exception as e:
            return {'status': f'erro: {str(e)[:50]}', 'distancia_km': None, 'tempo_minutos': None}
    
    def calculate_distances_custom(self, origem, lista_destinos, delay=0.2):
        """
        Calcula distâncias de uma origem para lista personalizada de destinos
        """
        print(f"🚀 Calculando distâncias de {origem}")
        print(f"📋 Para {len(lista_destinos)} destinos personalizados")
        print("="*60)
        
        # 1. Buscar coordenadas da origem
        print(f"🗺️ Localizando origem: {origem}")
        origin_coords = self.get_coordinates(origem)
        
        if not origin_coords:
            print(f"❌ Não foi possível encontrar a origem: {origem}")
            return []
        
        print(f"✅ {origem}: {origin_coords[1]:.4f}, {origin_coords[0]:.4f}")
        
        # 2. Processar cada destino
        resultados = []
        sucessos = 0
        erros = 0
        
        print(f"\n📏 Calculando distâncias...")
        
        for i, destino in enumerate(lista_destinos, 1):
            print(f"   {i:3d}/{len(lista_destinos)} - {destino:<25}", end=" ")
            
            # Busca coordenadas do destino
            dest_coords = self.get_coordinates(destino)
            
            if not dest_coords:
                print("❌ Coordenadas não encontradas")
                resultados.append({
                    'cidade_origem': origem,
                    'cidade_destino': destino,
                    'distancia_km': None,
                    'tempo_minutos': None,
                    'status': 'coordenadas_nao_encontradas',
                    'coordenadas_origem': f"{origin_coords[1]:.4f}, {origin_coords[0]:.4f}",
                    'coordenadas_destino': None
                })
                erros += 1
                continue
            
            # Calcula distância rodoviária
            resultado_rota = self.calculate_route_distance(origin_coords, dest_coords)
            
            resultados.append({
                'cidade_origem': origem,
                'cidade_destino': destino,
                'distancia_km': resultado_rota['distancia_km'],
                'tempo_minutos': resultado_rota['tempo_minutos'],
                'status': resultado_rota['status'],
                'coordenadas_origem': f"{origin_coords[1]:.4f}, {origin_coords[0]:.4f}",
                'coordenadas_destino': f"{dest_coords[1]:.4f}, {dest_coords[0]:.4f}"
            })
            
            if resultado_rota['status'] == 'sucesso':
                print(f"✅ {resultado_rota['distancia_km']:>6} km ({resultado_rota['tempo_minutos']:>3.0f} min)")
                sucessos += 1
            else:
                print(f"❌ {resultado_rota['status']}")
                erros += 1
            
            # Rate limiting
            time.sleep(delay)
        
        print(f"\n✅ Processamento concluído!")
        print(f"   ✅ Sucessos: {sucessos}")
        print(f"   ❌ Erros: {erros}")
        
        return resultados
    
    def save_results_to_excel(self, resultados, origem, filename=None):
        """
        Salva resultados em Excel com análises
        """
        if not resultados:
            print("❌ Nenhum resultado para salvar.")
            return None
        
        if not filename:
            origem_clean = origem.replace(" ", "_").replace("/", "_")
            filename = f'distancias_{origem_clean}_personalizada.xlsx'
        
        # Filtrar apenas sucessos para estatísticas
        sucessos = [r for r in resultados if r['distancia_km'] is not None]
        
        if not sucessos:
            print("❌ Nenhuma distância calculada com sucesso.")
            return None
        
        # Criar DataFrame
        df_all = pd.DataFrame(resultados)
        df_sucessos = pd.DataFrame(sucessos).sort_values('distancia_km')
        
        # Estatísticas
        stats = {
            'Cidade Origem': origem,
            'Total Solicitado': len(resultados),
            'Calculados com Sucesso': len(sucessos),
            'Erros': len(resultados) - len(sucessos),
            'Taxa de Sucesso (%)': round((len(sucessos) / len(resultados)) * 100, 1),
            'Distância Média (km)': round(df_sucessos['distancia_km'].mean(), 2),
            'Distância Máxima (km)': df_sucessos['distancia_km'].max(),
            'Distância Mínima (km)': df_sucessos['distancia_km'].min(),
            'Tempo Médio (min)': round(df_sucessos['tempo_minutos'].mean(), 0),
            'Cidade Mais Próxima': df_sucessos.iloc[0]['cidade_destino'],
            'Distância Mais Próxima (km)': df_sucessos.iloc[0]['distancia_km'],
            'Cidade Mais Distante': df_sucessos.iloc[-1]['cidade_destino'],
            'Distância Mais Distante (km)': df_sucessos.iloc[-1]['distancia_km']
        }
        
        # Salvar em Excel
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Aba principal - todos os resultados
            df_all.to_excel(writer, sheet_name='Todos_Resultados', index=False)
            
            # Aba apenas sucessos ordenados
            df_sucessos.to_excel(writer, sheet_name='Distancias_Ordenadas', index=False)
            
            # Resumo/Estatísticas
            stats_df = pd.DataFrame(list(stats.items()), columns=['Métrica', 'Valor'])
            stats_df.to_excel(writer, sheet_name='Resumo', index=False)
            
            # Análise por faixas (apenas sucessos)
            if len(sucessos) > 0:
                faixas = [
                    ("Até_50km", df_sucessos[df_sucessos['distancia_km'] <= 50]),
                    ("51_100km", df_sucessos[(df_sucessos['distancia_km'] > 50) & (df_sucessos['distancia_km'] <= 100)]),
                    ("101_200km", df_sucessos[(df_sucessos['distancia_km'] > 100) & (df_sucessos['distancia_km'] <= 200)]),
                    ("201_500km", df_sucessos[(df_sucessos['distancia_km'] > 200) & (df_sucessos['distancia_km'] <= 500)]),
                    ("Acima_500km", df_sucessos[df_sucessos['distancia_km'] > 500])
                ]
                
                for nome_faixa, dados_faixa in faixas:
                    if not dados_faixa.empty:
                        dados_faixa.to_excel(writer, sheet_name=nome_faixa, index=False)
            
            # Erros (se houver)
            erros_df = df_all[df_all['distancia_km'].isna()]
            if not erros_df.empty:
                erros_df.to_excel(writer, sheet_name='Erros', index=False)
        
        # Relatório no console
        print(f"\n💾 Arquivo salvo: {filename}")
        print(f"\n📊 RESUMO:")
        print(f"   🎯 Origem: {origem}")
        print(f"   📋 Solicitados: {stats['Total Solicitado']}")
        print(f"   ✅ Calculados: {stats['Calculados com Sucesso']}")
        print(f"   📈 Taxa de sucesso: {stats['Taxa de Sucesso (%)']}%")
        
        if sucessos:
            print(f"   📏 Distância média: {stats['Distância Média (km)']} km")
            print(f"   🔸 Mais próxima: {stats['Cidade Mais Próxima']} ({stats['Distância Mais Próxima (km)']} km)")
            print(f"   🔹 Mais distante: {stats['Cidade Mais Distante']} ({stats['Distância Mais Distante (km)']} km)")
            
            # Top 5 mais próximas
            print(f"\n🏆 TOP 5 MAIS PRÓXIMAS:")
            for i, row in df_sucessos.head(5).iterrows():
                print(f"   {i+1}. {row['cidade_destino']:<20} - {row['distancia_km']:>6} km")
        
        return filename

# FUNÇÃO PRINCIPAL PARA USO FÁCIL
def calcular_distancias_personalizada(cidade_origem, lista_cidades_destino):
    """
    Função principal para calcular distâncias personalizadas
    
    Parâmetros:
    - cidade_origem: string com nome da cidade de partida
    - lista_cidades_destino: lista de strings com nomes das cidades destino
    
    Exemplo:
    resultado = calcular_distancias_personalizada(
        "Juiz de Fora", 
        ["Belo Horizonte", "Rio de Janeiro", "São Paulo", "Uberlândia"]
    )
    """
    
    print("🚀 CALCULADORA DE DISTÂNCIAS PERSONALIZADA")
    print("="*60)
    
    # Validações básicas
    if not cidade_origem or not cidade_origem.strip():
        print("❌ Cidade de origem não informada!")
        return None
    
    if not lista_cidades_destino or len(lista_cidades_destino) == 0:
        print("❌ Lista de destinos vazia!")
        return None
    
    # Remove duplicatas e espaços em branco
    lista_limpa = list(set([cidade.strip() for cidade in lista_cidades_destino if cidade.strip()]))
    
    print(f"📍 Origem: {cidade_origem}")
    print(f"📋 Destinos: {len(lista_limpa)} cidades")
    print(f"⏱️ Tempo estimado: {len(lista_limpa) * 0.3:.1f} minutos")
    
    # Inicializar calculadora
    calc = DistanceCalculatorCustom()
    
    # Calcular distâncias
    resultados = calc.calculate_distances_custom(cidade_origem, lista_limpa)
    
    # Salvar em Excel
    if resultados:
        filename = calc.save_results_to_excel(resultados, cidade_origem)
        return filename
    else:
        print("❌ Nenhum resultado obtido.")
        return None

# EXEMPLO DE USO
if __name__ == "__main__":
    # EXEMPLO 1: Juiz de Fora para principais cidades de MG
    origem = "Juiz de Fora"
    destinos_mg = [
        "Belo Horizonte", "Uberlândia", "Contagem", "Betim", "Montes Claros",
        "Ribeirão das Neves", "Uberaba", "Governador Valadares", "Ipatinga",
        "Sete Lagoas", "Divinópolis", "Santa Luzia", "Ibirité", "Poços de Caldas",
        "Patos de Minas", "Pouso Alegre", "Teófilo Otoni", "Barbacena",
        "Sabará", "Vespasiano", "Araguari", "Conselheiro Lafaiete",
        "Itabira", "Passos", "Coronel Fabriciano", "Muriaé"
    ]
    
    print("🧪 EXEMPLO: Distâncias de Juiz de Fora para principais cidades de MG")
    resultado = calcular_distancias_personalizada(origem, destinos_mg)
    
    if resultado:
        print(f"\n🎉 Exemplo concluído! Arquivo: {resultado}")
        print("\n" + "="*60)
        print("🔧 COMO USAR COM SUAS PRÓPRIAS LISTAS:")
        print("1. Defina sua cidade de origem")
        print("2. Crie sua lista de destinos")
        print("3. Execute: calcular_distancias_personalizada(origem, destinos)")
        print("4. Aguarde o processamento")
        print("5. Receba seu arquivo Excel com todas as análises!")