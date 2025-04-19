"""
Módulo para scraping de notícias de M&A do site Pipeline Valor.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import logging
import re
import time
from urllib.parse import urljoin

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger('pipeline_valor_scraper')

class PipelineValorScraper:
    """
    Classe para scraping de notícias de M&A do site Pipeline Valor.
    """
    
    BASE_URL = "https://pipelinevalor.globo.com"
    SEARCH_URL = f"{BASE_URL}/busca/"
    
    def __init__(self):
        """
        Inicializa o scraper com headers padrão para simular um navegador.
        """
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def search_news(self, query="fusão aquisição M&A", days=30, max_pages=5):
        """
        Busca notícias relacionadas a M&A no Pipeline Valor.
        
        Args:
            query (str): Termos de busca.
            days (int): Número de dias para buscar notícias (a partir de hoje).
            max_pages (int): Número máximo de páginas de resultados a serem processadas.
            
        Returns:
            list: Lista de URLs de notícias encontradas.
        """
        logger.info(f"Buscando notícias com query: '{query}', dos últimos {days} dias")
        
        # Calcular a data limite
        date_limit = datetime.now() - timedelta(days=days)
        
        # Preparar parâmetros de busca
        search_params = {
            'q': query,
            'page': 1
        }
        
        all_news_urls = []
        
        # Iterar pelas páginas de resultados
        for page in range(1, max_pages + 1):
            search_params['page'] = page
            
            try:
                logger.info(f"Buscando página {page} de resultados")
                response = self.session.get(self.SEARCH_URL, params=search_params)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Encontrar todos os resultados de notícias
                news_items = soup.select('.search-result-item')
                
                if not news_items:
                    logger.info(f"Nenhum resultado encontrado na página {page}")
                    break
                
                # Processar cada item de notícia
                for item in news_items:
                    # Extrair data da publicação
                    date_elem = item.select_one('.search-result-date')
                    if not date_elem:
                        continue
                    
                    # Converter texto de data para objeto datetime
                    try:
                        # Formato típico: "DD/MM/AAAA às HH:MM"
                        date_text = date_elem.text.strip()
                        date_part = re.search(r'(\d{2}/\d{2}/\d{4})', date_text)
                        
                        if date_part:
                            pub_date = datetime.strptime(date_part.group(1), '%d/%m/%Y')
                            
                            # Verificar se a notícia está dentro do período desejado
                            if pub_date < date_limit:
                                logger.info(f"Atingida data limite ({date_limit.strftime('%d/%m/%Y')})")
                                break
                            
                            # Extrair URL da notícia
                            link_elem = item.select_one('a.search-result-link')
                            if link_elem and 'href' in link_elem.attrs:
                                news_url = urljoin(self.BASE_URL, link_elem['href'])
                                all_news_urls.append(news_url)
                                logger.debug(f"Adicionada notícia: {news_url}")
                    except Exception as e:
                        logger.error(f"Erro ao processar data da notícia: {e}")
                
                # Se chegamos à data limite, paramos a busca
                if pub_date < date_limit:
                    break
                
                # Pausa para não sobrecarregar o servidor
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Erro ao buscar página {page}: {e}")
                break
        
        logger.info(f"Total de {len(all_news_urls)} notícias encontradas")
        return all_news_urls
    
    def extract_news_data(self, url):
        """
        Extrai informações detalhadas de uma notícia específica.
        
        Args:
            url (str): URL da notícia.
            
        Returns:
            dict: Dicionário com dados extraídos ou None se falhar.
        """
        logger.info(f"Extraindo dados da notícia: {url}")
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Extrair título
            title = soup.select_one('h1.article-title')
            title_text = title.text.strip() if title else "Título não encontrado"
            
            # Extrair data
            date_elem = soup.select_one('time.article-date')
            date_text = date_elem.text.strip() if date_elem else "Data não encontrada"
            
            # Extrair conteúdo completo
            content_elem = soup.select_one('.article-content')
            content_text = content_elem.text.strip() if content_elem else ""
            
            # Inicializar dados da transação
            transaction_data = {
                'url': url,
                'title': title_text,
                'date': date_text,
                'buyer': None,
                'acquired': None,
                'value': None,
                'multiple': None
            }
            
            # Extrair dados da transação do conteúdo
            if content_text:
                # Buscar empresa compradora
                buyer_patterns = [
                    r'(?:A|a) ([\w\s]+) (?:adquiriu|comprou|anunciou a compra)',
                    r'([\w\s]+) (?:fechou acordo|assinou contrato) para (?:adquirir|comprar)',
                    r'([\w\s]+) (?:anunciou|comunicou|informou) (?:a aquisição|a compra)'
                ]
                
                for pattern in buyer_patterns:
                    buyer_match = re.search(pattern, content_text)
                    if buyer_match:
                        transaction_data['buyer'] = buyer_match.group(1).strip()
                        break
                
                # Buscar empresa adquirida
                acquired_patterns = [
                    r'(?:adquiriu|comprou|anunciou a compra de) (?:a empresa |a |)([\w\s]+)',
                    r'(?:aquisição|compra) d[ao] ([\w\s]+)',
                    r'(?:adquirir|comprar) (?:a empresa |a |)([\w\s]+)'
                ]
                
                for pattern in acquired_patterns:
                    acquired_match = re.search(pattern, content_text)
                    if acquired_match:
                        transaction_data['acquired'] = acquired_match.group(1).strip()
                        break
                
                # Buscar valor da transação
                value_patterns = [
                    r'(?:valor|montante|preço) de (?:R\$|USD|US\$|€|EUR) ([\d,\.]+)(?:\s*)(milhões|bilhões|milhão|bilhão)',
                    r'(?:R\$|USD|US\$|€|EUR) ([\d,\.]+)(?:\s*)(milhões|bilhões|milhão|bilhão)',
                    r'transação de (?:R\$|USD|US\$|€|EUR) ([\d,\.]+)(?:\s*)(milhões|bilhões|milhão|bilhão)'
                ]
                
                for pattern in value_patterns:
                    value_match = re.search(pattern, content_text)
                    if value_match:
                        value_num = value_match.group(1).replace('.', '').replace(',', '.')
                        value_unit = value_match.group(2)
                        
                        # Determinar a moeda
                        currency_match = re.search(r'(R\$|USD|US\$|€|EUR)', content_text)
                        currency = currency_match.group(1) if currency_match else "R$"
                        
                        # Formatar o valor
                        transaction_data['value'] = f"{currency} {value_num} {value_unit}"
                        break
                
                # Buscar múltiplo
                multiple_patterns = [
                    r'([\d,\.]+)(?:\s*)(?:x|vezes)(?:\s*)(?:o |a |)(EBITDA|receita|faturamento)',
                    r'múltiplo de ([\d,\.]+)(?:\s*)(?:x|vezes)(?:\s*)(?:o |a |)(EBITDA|receita|faturamento)',
                    r'([\d,\.]+)(?:\s*)(?:x|vezes) (?:o valor|a cifra)'
                ]
                
                for pattern in multiple_patterns:
                    multiple_match = re.search(pattern, content_text)
                    if multiple_match:
                        multiple_num = multiple_match.group(1).replace(',', '.')
                        multiple_base = multiple_match.group(2) if len(multiple_match.groups()) > 1 else ""
                        
                        if multiple_base:
                            transaction_data['multiple'] = f"{multiple_num}x {multiple_base}"
                        else:
                            transaction_data['multiple'] = f"{multiple_num}x"
                        break
            
            return transaction_data
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados da notícia {url}: {e}")
            return None
    
    def run_scraper(self, query="fusão aquisição M&A", days=30, max_pages=5):
        """
        Executa o processo completo de scraping.
        
        Args:
            query (str): Termos de busca.
            days (int): Número de dias para buscar notícias.
            max_pages (int): Número máximo de páginas de resultados.
            
        Returns:
            pd.DataFrame: DataFrame com os dados extraídos.
        """
        # Buscar URLs de notícias
        news_urls = self.search_news(query, days, max_pages)
        
        # Extrair dados de cada notícia
        all_data = []
        for url in news_urls:
            news_data = self.extract_news_data(url)
            if news_data:
                all_data.append(news_data)
            # Pausa para não sobrecarregar o servidor
            time.sleep(3)
        
        # Criar DataFrame
        if all_data:
            df = pd.DataFrame(all_data)
            logger.info(f"Extração concluída. {len(df)} notícias processadas.")
            return df
        else:
            logger.warning("Nenhum dado extraído.")
            return pd.DataFrame()

# Função para teste
def test_pipeline_valor_scraper():
    """
    Testa o scraper do Pipeline Valor.
    """
    scraper = PipelineValorScraper()
    df = scraper.run_scraper(days=30, max_pages=2)
    
    if not df.empty:
        print(f"Dados extraídos: {len(df)} notícias")
        print(df.head())
        
        # Salvar para teste
        df.to_excel("pipeline_valor_test.xlsx", index=False)
        print("Dados salvos em 'pipeline_valor_test.xlsx'")
    else:
        print("Nenhum dado extraído")

if __name__ == "__main__":
    test_pipeline_valor_scraper()
