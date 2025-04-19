"""
Módulo principal otimizado para extração e integração de dados de M&A de múltiplas fontes.
"""

import pandas as pd
import logging
import os
from datetime import datetime
import time
import concurrent.futures
from dotenv import load_dotenv
import traceback

# Importar os scrapers otimizados
from scrapers.pipeline_valor import PipelineValorScraper
from scrapers.valor_economico_optimized import ValorEconomicoScraper
from scrapers.fusoes_aquisicoes_optimized import FusoesAquisicoesScraper

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ma_extractor_optimized.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('ma_extractor_optimized')

class MAExtractorOptimized:
    """
    Classe otimizada para extração e integração de dados de M&A de múltiplas fontes.
    """
    
    def __init__(self, output_dir="./output"):
        """
        Inicializa o extrator de dados de M&A.
        
        Args:
            output_dir (str): Diretório para salvar os resultados.
        """
        self.output_dir = output_dir
        
        # Criar diretório de saída se não existir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Diretório de saída criado: {output_dir}")
    
    def run_pipeline_valor(self, query="fusão aquisição M&A", days=30, max_pages=5):
        """
        Executa o scraper do Pipeline Valor.
        
        Args:
            query (str): Termos de busca.
            days (int): Número de dias para buscar notícias.
            max_pages (int): Número máximo de páginas de resultados.
            
        Returns:
            pd.DataFrame: DataFrame com os dados extraídos.
        """
        logger.info("Iniciando extração de dados do Pipeline Valor")
        
        try:
            scraper = PipelineValorScraper()
            df = scraper.run_scraper(query=query, days=days, max_pages=max_pages)
            
            if not df.empty:
                df['source'] = 'Pipeline Valor'
                logger.info(f"Extração do Pipeline Valor concluída: {len(df)} notícias")
            else:
                logger.warning("Nenhum dado extraído do Pipeline Valor")
            
            return df
        
        except Exception as e:
            logger.error(f"Erro na extração do Pipeline Valor: {e}")
            logger.error(traceback.format_exc())
            return pd.DataFrame()
    
    def run_valor_economico(self, query="fusão aquisição M&A", days=30, max_pages=5):
        """
        Executa o scraper otimizado do Valor Econômico.
        
        Args:
            query (str): Termos de busca.
            days (int): Número de dias para buscar notícias.
            max_pages (int): Número máximo de páginas de resultados.
            
        Returns:
            pd.DataFrame: DataFrame com os dados extraídos.
        """
        logger.info("Iniciando extração de dados do Valor Econômico")
        
        # Verificar se as credenciais estão configuradas
        if not os.getenv('VALOR_EMAIL') or not os.getenv('VALOR_PASSWORD'):
            logger.error("Credenciais do Valor Econômico não configuradas. Configure VALOR_EMAIL e VALOR_PASSWORD no arquivo .env")
            return pd.DataFrame()
        
        try:
            # Usar versão otimizada com fallback para requests
            scraper = ValorEconomicoScraper(headless=True, use_requests_fallback=True)
            df = scraper.run_scraper(query=query, days=days, max_pages=max_pages)
            
            if not df.empty:
                df['source'] = 'Valor Econômico'
                logger.info(f"Extração do Valor Econômico concluída: {len(df)} notícias")
            else:
                logger.warning("Nenhum dado extraído do Valor Econômico")
            
            return df
        
        except Exception as e:
            logger.error(f"Erro na extração do Valor Econômico: {e}")
            logger.error(traceback.format_exc())
            return pd.DataFrame()
        
    def run_fusoes_aquisicoes(self, query="fusão aquisição M&A", days=30, max_pages=5):
        """
        Executa o scraper otimizado do Fusões e Aquisições.
        
        Args:
            query (str): Termos de busca.
            days (int): Número de dias para buscar notícias.
            max_pages (int): Número máximo de páginas de resultados.
            
        Returns:
            pd.DataFrame: DataFrame com os dados extraídos.
        """
        logger.info("Iniciando extração de dados do Fusões e Aquisições")
        
        try:
            # Usar versão otimizada com métodos alternativos de busca
            scraper = FusoesAquisicoesScraper()
            df = scraper.run_scraper(query=query, days=days, max_pages=max_pages)
            
            if not df.empty:
                df['source'] = 'Fusões e Aquisições'
                logger.info(f"Extração do Fusões e Aquisições concluída: {len(df)} notícias")
            else:
                logger.warning("Nenhum dado extraído do Fusões e Aquisições")
            
            return df
        
        except Exception as e:
            logger.error(f"Erro na extração do Fusões e Aquisições: {e}")
            logger.error(traceback.format_exc())
            return pd.DataFrame()
    
    def run_all_sequential(self, query="fusão aquisição M&A", days=30, max_pages=5):
        """
        Executa todos os scrapers sequencialmente.
        
        Args:
            query (str): Termos de busca.
            days (int): Número de dias para buscar notícias.
            max_pages (int): Número máximo de páginas de resultados.
            
        Returns:
            pd.DataFrame: DataFrame combinado com os dados extraídos de todas as fontes.
        """
        logger.info(f"Iniciando extração sequencial de todas as fontes. Query: '{query}', Dias: {days}")
        
        # Executar cada scraper
        df_pipeline = self.run_pipeline_valor(query, days, max_pages)
        df_valor = self.run_valor_economico(query, days, max_pages)
        df_fusoes = self.run_fusoes_aquisicoes(query, days, max_pages)
        
        # Combinar os resultados
        dfs = [df for df in [df_pipeline, df_valor, df_fusoes] if not df.empty]
        
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Extração combinada concluída: {len(combined_df)} notícias no total")
            return combined_df
        else:
            logger.warning("Nenhum dado extraído de nenhuma fonte")
            return pd.DataFrame()
    
    def run_all_parallel(self, query="fusão aquisição M&A", days=30, max_pages=5):
        """
        Executa todos os scrapers em paralelo.
        
        Args:
            query (str): Termos de busca.
            days (int): Número de dias para buscar notícias.
            max_pages (int): Número máximo de páginas de resultados.
            
        Returns:
            pd.DataFrame: DataFrame combinado com os dados extraídos de todas as fontes.
        """
        logger.info(f"Iniciando extração paralela de todas as fontes. Query: '{query}', Dias: {days}")
        
        # Definir funções para execução paralela
        def run_pipeline():
            try:
                return self.run_pipeline_valor(query, days, max_pages)
            except Exception as e:
                logger.error(f"Erro na execução paralela do Pipeline Valor: {e}")
                return pd.DataFrame()
        
        def run_valor():
            try:
                return self.run_valor_economico(query, days, max_pages)
            except Exception as e:
                logger.error(f"Erro na execução paralela do Valor Econômico: {e}")
                return pd.DataFrame()
        
        def run_fusoes():
            try:
                return self.run_fusoes_aquisicoes(query, days, max_pages)
            except Exception as e:
                logger.error(f"Erro na execução paralela do Fusões e Aquisições: {e}")
                return pd.DataFrame()
        
        # Executar em paralelo
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # Submeter tarefas
            future_pipeline = executor.submit(run_pipeline)
            future_valor = executor.submit(run_valor)
            future_fusoes = executor.submit(run_fusoes)
            
            # Coletar resultados com tratamento de exceções
            for future in concurrent.futures.as_completed([future_pipeline, future_valor, future_fusoes]):
                try:
                    result = future.result()
                    if not result.empty:
                        results.append(result)
                except Exception as e:
                    logger.error(f"Erro ao obter resultado de execução paralela: {e}")
        
        # Combinar os resultados
        if results:
            combined_df = pd.concat(results, ignore_index=True)
            logger.info(f"Extração paralela concluída: {len(combined_df)} notícias no total")
            return combined_df
        else:
            logger.warning("Nenhum dado extraído de nenhuma fonte na execução paralela")
            return pd.DataFrame()
    
    def clean_and_structure_data(self, df):
        """
        Limpa e estrutura os dados extraídos.
        
        Args:
            df (pd.DataFrame): DataFrame com os dados extraídos.
            
        Returns:
            pd.DataFrame: DataFrame com os dados limpos e estruturados.
        """
        if df.empty:
            logger.warning("Nenhum dado para limpar e estruturar")
            return df
        
        logger.info("Iniciando limpeza e estruturação dos dados")
        
        try:
            # Criar cópia para não modificar o original
            clean_df = df.copy()
            
            # Remover duplicatas com base na URL
            initial_len = len(clean_df)
            clean_df.drop_duplicates(subset=['url'], inplace=True)
            logger.info(f"Removidas {initial_len - len(clean_df)} notícias duplicadas")
            
            # Padronizar formato de data
            def standardize_date(date_str):
                if not date_str or date_str == "Data não encontrada":
                    return None
                
                try:
                    # Tentar extrair data no formato DD/MM/AAAA
                    import re
                    date_match = re.search(r'(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{2,4})', date_str)
                    
                    if date_match:
                        day, month, year = date_match.groups()
                        
                        # Ajustar ano se necessário
                        if len(year) == 2:
                            year = f"20{year}"
                        
                        return f"{day.zfill(2)}/{month.zfill(2)}/{year}"
                    
                    # Tentar formato "DD de mês de AAAA"
                    date_parts = re.findall(r'(\d+) de (\w+) de (\d{4})', date_str)
                    
                    if date_parts:
                        day, month_name, year = date_parts[0]
                        
                        # Converter nome do mês para número
                        month_map = {
                            'janeiro': '01', 'fevereiro': '02', 'março': '03', 'abril': '04',
                            'maio': '05', 'junho': '06', 'julho': '07', 'agosto': '08',
                            'setembro': '09', 'outubro': '10', 'novembro': '11', 'dezembro': '12'
                        }
                        
                        month = month_map.get(month_name.lower(), '01')
                        return f"{day.zfill(2)}/{month}/{year}"
                    
                    return date_str
                
                except Exception:
                    return date_str
            
            clean_df['date_standardized'] = clean_df['date'].apply(standardize_date)
            
            # Preencher valores nulos com "Não informado"
            for col in ['buyer', 'acquired', 'value', 'multiple']:
                clean_df[col] = clean_df[col].fillna("Não informado")
            
            # Ordenar por data (mais recentes primeiro)
            clean_df.sort_values(by='date_standardized', ascending=False, inplace=True)
            
            # Adicionar coluna de data de extração
            clean_df['extraction_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            logger.info("Limpeza e estruturação dos dados concluída")
            return clean_df
            
        except Exception as e:
            logger.error(f"Erro na limpeza e estruturação dos dados: {e}")
            logger.error(traceback.format_exc())
            return df
    
    def save_to_excel(self, df, filename=None):
        """
        Salva os dados em um arquivo Excel.
        
        Args:
            df (pd.DataFrame): DataFrame com os dados.
            filename (str): Nome do arquivo (opcional).
            
        Returns:
            str: Caminho do arquivo salvo.
        """
        if df.empty:
            logger.warning("Nenhum dado para salvar em Excel")
            return None
        
        try:
            # Gerar nome de arquivo se não fornecido
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"ma_noticias_{timestamp}.xlsx"
            
            # Garantir que tenha extensão .xlsx
            if not filename.endswith('.xlsx'):
                filename += '.xlsx'
            
            # Caminho completo do arquivo
            file_path = os.path.join(self.output_dir, filename)
            
            # Salvar em Excel
            df.to_excel(file_path, index=False)
            logger.info(f"Dados salvos em {file_path}")
            
            return file_path
            
        except Exception as e:
            logger.error(f"Erro ao salvar dados em Excel: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def run_extraction(self, query="fusão aquisição M&A", days=30, max_pages=5, parallel=True, max_retries=3):
        """
        Executa o processo completo de extração, limpeza e salvamento dos dados.
        
        Args:
            query (str): Termos de busca.
            days (int): Número de dias para buscar notícias.
            max_pages (int): Número máximo de páginas de resultados.
            parallel (bool): Se True, executa os scrapers em paralelo.
            max_retries (int): Número máximo de tentativas em caso de falha.
            
        Returns:
            str: Caminho do arquivo Excel salvo.
        """
        logger.info(f"Iniciando processo completo de extração. Parallel: {parallel}, Max retries: {max_retries}")
        
        start_time = time.time()
        
        # Implementar retry para aumentar a confiabilidade
        for attempt in range(max_retries):
            try:
                # Executar scrapers
                if parallel:
                    df = self.run_all_parallel(query, days, max_pages)
                else:
                    df = self.run_all_sequential(query, days, max_pages)
                
                # Verificar se obtivemos algum resultado
                if df.empty and attempt < max_retries - 1:
                    logger.warning(f"Nenhum dado extraído na tentativa {attempt+1}/{max_retries}. Tentando novamente...")
                    time.sleep(10)  # Esperar antes de tentar novamente
                    continue
                
                # Limpar e estruturar dados
                clean_df = self.clean_and_structure_data(df)
                
                # Salvar em Excel
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_path = self.save_to_excel(clean_df, f"ma_noticias_{timestamp}.xlsx")
                
                end_time = time.time()
                duration = end_time - start_time
                
                logger.info(f"Processo completo finalizado em {duration:.2f} segundos")
                logger.info(f"Total de notícias extraídas: {len(clean_df)}")
                
                return file_path
                
            except Exception as e:
                logger.error(f"Erro na tentativa {attempt+1}/{max_retries}: {e}")
                logger.error(traceback.format_exc())
                
                if attempt < max_retries - 1:
                    wait_time = 30 * (attempt + 1)  # Backoff exponencial
                    logger.info(f"Aguardando {wait_time} segundos antes da próxima tentativa")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Falha após {max_retries} tentativas")
        
        return None

# Função para teste
def test_ma_extractor_optimized():
    """
    Testa o extrator otimizado de dados de M&A.
    """
    extractor = MAExtractorOptimized(output_dir="./output")
    file_path = extractor.run_extraction(days=30, max_pages=2, parallel=False, max_retries=2)
    
    if file_path:
        print(f"Extração concluída com sucesso. Arquivo salvo em: {file_path}")
    else:
        print("Falha na extração de dados")

if __name__ == "__main__":
    test_ma_extractor_optimized()
