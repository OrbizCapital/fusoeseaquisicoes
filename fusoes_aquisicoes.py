"""
Módulo para scraping de notícias de M&A do site Fusões e Aquisições.
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

logger = logging.getLogger('fusoes_aquisicoes_scraper')

class FusoesAquisicoesScraper:
    """
    Classe para scraping de notícias de M&A do site Fusões e Aquisições.
    """
    
    BASE_URL = "https://fusoesaquisicoes.com"
    SEARCH_URL = f"{BASE_URL}/search"
    NEWS_URL = f"{BASE_URL}/category/noticias"
    
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
        Busca notícias relacionadas a M&A no site Fusões e Aquisições.
        
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
        
        all_news_urls = []
        
        # Estratégia 1: Buscar pela seção de notícias recentes
        try:
            logger.info("Buscando na seção de notícias recentes")
            
            for page in range(1, max_pages + 1):
                page_url = f"{self.NEWS_URL}/page/{page}/" if page > 1 else self.NEWS_URL
                
                logger.info(f"Acessando página {page}: {page_url}")
                response = self.session.get(page_url)
                
                if response.status_code != 200:
                    logger.warning(f"Erro ao acessar página {page}: {response.status_code}")
                    break
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Encontrar artigos na página
                articles = soup.select('article.post')
                
                if not articles:
                    logger.info(f"Nenhum artigo encontrado na página {page}")
                    break
                
                # Processar cada artigo
                for article in articles:
                    # Extrair data
                    date_elem = article.select_one('.entry-date')
                    if not date_elem:
                        continue
                    
                    # Converter texto de data para objeto datetime
                    try:
                        date_text = date_elem.text.strip()
                        # Formato típico: "DD de mês de AAAA"
                        date_parts = re.findall(r'(\d+) de (\w+) de (\d{4})', date_text)
                        
                        if date_parts:
                            day, month_name, year = date_parts[0]
                            
                            # Converter nome do mês para número
                            month_map = {
                                'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4,
                                'maio': 5, 'junho': 6, 'julho': 7, 'agosto': 8,
                                'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12
                            }
                            
                            month = month_map.get(month_name.lower(), 1)
                            pub_date = datetime(int(year), month, int(day))
                            
                            # Verificar se está dentro do período desejado
                            if pub_date < date_limit:
                                logger.info(f"Atingida data limite ({date_limit.strftime('%d/%m/%Y')})")
                                break
                            
                            # Extrair URL do artigo
                            link_elem = article.select_one('h2.entry-title a')
                            if link_elem and 'href' in link_elem.attrs:
                                news_url = link_elem['href']
                                
                                # Verificar se a URL contém palavras-chave da busca
                                if self._check_keywords(news_url, query) or self._check_keywords(link_elem.text, query):
                                    all_news_urls.append(news_url)
                                    logger.debug(f"Adicionada notícia: {news_url}")
                    except Exception as e:
                        logger.error(f"Erro ao processar data do artigo: {e}")
                
                # Se chegamos à data limite, paramos a busca
                if pub_date < date_limit:
                    break
                
                # Pausa para não sobrecarregar o servidor
                time.sleep(2)
                
        except Exception as e:
            logger.error(f"Erro ao buscar notícias recentes: {e}")
        
        # Estratégia 2: Usar a busca do site
        try:
            logger.info("Usando a busca do site com palavras-chave")
            
            # Dividir a query em palavras-chave
            keywords = query.split()
            
            for keyword in keywords:
                search_params = {'s': keyword}
                
                logger.info(f"Buscando por: {keyword}")
                response = self.session.get(self.SEARCH_URL, params=search_params)
                
                if response.status_code != 200:
                    logger.warning(f"Erro na busca por '{keyword}': {response.status_code}")
                    continue
                
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Encontrar artigos nos resultados da busca
                articles = soup.select('article.post')
                
                for article in articles:
                    # Extrair data
                    date_elem = article.select_one('.entry-date')
                    if not date_elem:
                        continue
                    
                    # Converter texto de data para objeto datetime
                    try:
                        date_text = date_elem.text.strip()
                        # Formato típico: "DD de mês de AAAA"
                        date_parts = re.findall(r'(\d+) de (\w+) de (\d{4})', date_text)
                        
                        if date_parts:
                            day, month_name, year = date_parts[0]
                            
                            # Converter nome do mês para número
                            month_map = {
                                'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4,
                                'maio': 5, 'junho': 6, 'julho': 7, 'agosto': 8,
                                'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12
                            }
                            
                            month = month_map.get(month_name.lower(), 1)
                            pub_date = datetime(int(year), month, int(day))
                            
                            # Verificar se está dentro do período desejado
                            if pub_date < date_limit:
                                continue
                            
                            # Extrair URL do artigo
                            link_elem = article.select_one('h2.entry-title a')
                            if link_elem and 'href' in link_elem.attrs:
                                news_url = link_elem['href']
                                
                                # Adicionar se ainda não estiver na lista
                                if news_url not in all_news_urls:
                                    all_news_urls.append(news_url)
                                    logger.debug(f"Adicionada notícia da busca: {news_url}")
                    except Exception as e:
                        logger.error(f"Erro ao processar data do artigo da busca: {e}")
                
                # Pausa para não sobrecarregar o servidor
                time.sleep(2)
                
        except Exception as e:
            logger.error(f"Erro ao usar busca do site: {e}")
        
        logger.info(f"Total de {len(all_news_urls)} notícias encontradas")
        return all_news_urls
    
    def _check_keywords(self, text, query):
        """
        Verifica se o texto contém palavras-chave da query.
        
        Args:
            text (str): Texto a ser verificado.
            query (str): Query com palavras-chave.
            
        Returns:
            bool: True se contém palavras-chave, False caso contrário.
        """
        text = text.lower()
        keywords = [k.lower() for k in query.split()]
        
        # Palavras-chave adicionais relacionadas a M&A
        ma_keywords = ['fusão', 'aquisição', 'fusões', 'aquisições', 'compra', 
                      'adquiriu', 'comprou', 'transação', 'm&a', 'merger', 'acquisition']
        
        # Verificar palavras-chave da query
        for keyword in keywords:
            if keyword in text:
                return True
        
        # Verificar palavras-chave de M&A
        for keyword in ma_keywords:
            if keyword in text:
                return True
                
        return False
    
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
            title = soup.select_one('h1.entry-title')
            title_text = title.text.strip() if title else "Título não encontrado"
            
            # Extrair data
            date_elem = soup.select_one('.entry-date')
            date_text = date_elem.text.strip() if date_elem else "Data não encontrada"
            
            # Extrair conteúdo completo
            content_elem = soup.select_one('.entry-content')
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
                    r'([\w\s]+) (?:anunciou|comunicou|informou) (?:a aquisição|a compra)',
                    r'([\w\s]+) (?:concluiu|finalizou) (?:a aquisição|a compra)',
                    r'([\w\s]+) (?:é a compradora|é a adquirente)'
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
                    r'(?:adquirir|comprar) (?:a empresa |a |)([\w\s]+)',
                    r'([\w\s]+) (?:foi adquirida|foi comprada)',
                    r'([\w\s]+) (?:é a empresa adquirida|é o alvo da aquisição)'
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
                    r'transação de (?:R\$|USD|US\$|€|EUR) ([\d,\.]+)(?:\s*)(milhões|bilhões|milhão|bilhão)',
                    r'avaliada em (?:R\$|USD|US\$|€|EUR) ([\d,\.]+)(?:\s*)(milhões|bilhões|milhão|bilhão)',
                    r'estimado em (?:R\$|USD|US\$|€|EUR) ([\d,\.]+)(?:\s*)(milhões|bilhões|milhão|bilhão)'
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
                    r'([\d,\.]+)(?:\s*)(?:x|vezes) (?:o valor|a cifra)',
                    r'equivale a ([\d,\.]+)(?:\s*)(?:x|vezes)'
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
def test_fusoes_aquisicoes_scraper():
    """
    Testa o scraper do site Fusões e Aquisições.
    """
    scraper = FusoesAquisicoesScraper()
    df = scraper.run_scraper(days=30, max_pages=2)
    
    if not df.empty:
        print(f"Dados extraídos: {len(df)} notícias")
        print(df.head())
        
        # Salvar para teste
        df.to_excel("fusoes_aquisicoes_test.xlsx", index=False)
        print("Dados salvos em 'fusoes_aquisicoes_test.xlsx'")
    else:
        print("Nenhum dado extraído")

if __name__ == "__main__":
    test_fusoes_aquisicoes_scraper()
