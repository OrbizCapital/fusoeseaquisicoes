"""
Módulo otimizado para scraping de notícias de M&A do site Fusões e Aquisições.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import logging
import re
import time
from urllib.parse import urljoin
import random

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger('fusoes_aquisicoes_scraper')

class FusoesAquisicoesScraper:
    """
    Classe otimizada para scraping de notícias de M&A do site Fusões e Aquisições.
    """
    
    BASE_URL = "https://fusoesaquisicoes.com"
    SEARCH_URL = f"{BASE_URL}/search"
    NEWS_URL = f"{BASE_URL}/category/noticias"
    
    def __init__(self):
        """
        Inicializa o scraper com headers aprimorados para simular um navegador real.
        """
        # Lista de user agents para rotação
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
        ]
        
        # Obter um user agent aleatório
        self.headers = {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://www.google.com/',  # Simular vindo do Google
            'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
            'sec-ch-ua-mobile': '?0',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'cross-site',
            'sec-fetch-user': '?1',
            'dnt': '1'
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Adicionar cookies iniciais
        self.session.cookies.set('visited', 'yes', domain='fusoesaquisicoes.com')
        self.session.cookies.set('PHPSESSID', self._generate_session_id(), domain='fusoesaquisicoes.com')
    
    def _generate_session_id(self):
        """
        Gera um ID de sessão aleatório.
        """
        return ''.join(random.choices('0123456789abcdef', k=32))
    
    def _rotate_user_agent(self):
        """
        Alterna para um novo user agent aleatório.
        """
        new_user_agent = random.choice(self.user_agents)
        self.session.headers.update({'User-Agent': new_user_agent})
        logger.debug(f"Rotacionado para novo User-Agent: {new_user_agent}")
    
    def _make_request(self, url, params=None, max_retries=3, backoff_factor=1.5):
        """
        Faz uma requisição HTTP com retry e backoff exponencial.
        
        Args:
            url (str): URL para acessar.
            params (dict): Parâmetros da requisição.
            max_retries (int): Número máximo de tentativas.
            backoff_factor (float): Fator de backoff para espera entre tentativas.
            
        Returns:
            requests.Response: Objeto de resposta ou None se falhar.
        """
        for attempt in range(max_retries):
            try:
                # Rotacionar user agent a cada tentativa
                if attempt > 0:
                    self._rotate_user_agent()
                
                # Adicionar delay aleatório para parecer mais humano
                time.sleep(random.uniform(1.0, 3.0))
                
                response = self.session.get(url, params=params, timeout=10)
                
                # Verificar se a resposta é um erro 403
                if response.status_code == 403:
                    logger.warning(f"Recebido 403 Forbidden na tentativa {attempt+1}/{max_retries}")
                    # Esperar mais tempo antes da próxima tentativa
                    wait_time = backoff_factor ** attempt * 2
                    time.sleep(wait_time)
                    continue
                
                # Para outros códigos de erro, também tentar novamente
                if response.status_code != 200:
                    logger.warning(f"Recebido status code {response.status_code} na tentativa {attempt+1}/{max_retries}")
                    wait_time = backoff_factor ** attempt * 2
                    time.sleep(wait_time)
                    continue
                
                return response
                
            except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                logger.error(f"Erro na requisição (tentativa {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    wait_time = backoff_factor ** attempt * 2
                    logger.info(f"Aguardando {wait_time:.2f} segundos antes da próxima tentativa")
                    time.sleep(wait_time)
        
        logger.error(f"Falha após {max_retries} tentativas para URL: {url}")
        return None
    
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
        
        # Tentar método alternativo - acessar página inicial
        try:
            logger.info("Tentando acessar a página inicial para encontrar notícias recentes")
            
            response = self._make_request(self.BASE_URL)
            
            if response and response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Buscar artigos recentes na página inicial
                articles = soup.select('article.post')
                
                if articles:
                    logger.info(f"Encontrados {len(articles)} artigos na página inicial")
                    
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
                                    
                                    # Verificar se a URL contém palavras-chave da busca
                                    if self._check_keywords(news_url, query) or self._check_keywords(link_elem.text, query):
                                        all_news_urls.append(news_url)
                                        logger.debug(f"Adicionada notícia: {news_url}")
                        except Exception as e:
                            logger.error(f"Erro ao processar data do artigo: {e}")
                else:
                    logger.warning("Nenhum artigo encontrado na página inicial")
            else:
                logger.warning("Não foi possível acessar a página inicial")
                
        except Exception as e:
            logger.error(f"Erro ao acessar a página inicial: {e}")
        
        # Estratégia alternativa - usar Google para buscar notícias no site
        if not all_news_urls:
            try:
                logger.info("Tentando usar Google para buscar notícias no site")
                
                # Formatar query para Google
                google_query = f"site:fusoesaquisicoes.com {query}"
                google_url = "https://www.google.com/search"
                
                params = {
                    'q': google_query,
                    'num': 10,  # Número de resultados
                    'hl': 'pt-BR',  # Idioma
                    'tbs': f'qdr:m{days//30}'  # Filtro de tempo aproximado
                }
                
                response = self._make_request(google_url, params=params)
                
                if response and response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'lxml')
                    
                    # Extrair links dos resultados do Google
                    for result in soup.select('div.g'):
                        link = result.select_one('a')
                        if link and 'href' in link.attrs:
                            url = link['href']
                            
                            # Verificar se é uma URL do site Fusões e Aquisições
                            if 'fusoesaquisicoes.com' in url and url not in all_news_urls:
                                all_news_urls.append(url)
                                logger.debug(f"Adicionada notícia via Google: {url}")
                
                logger.info(f"Encontradas {len(all_news_urls)} notícias via Google")
                
            except Exception as e:
                logger.error(f"Erro ao usar Google para buscar notícias: {e}")
        
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
        if not text:
            return False
            
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
            response = self._make_request(url)
            
            if not response or response.status_code != 200:
                logger.error(f"Não foi possível acessar a notícia: {url}")
                return None
            
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
            time.sleep(random.uniform(2.0, 4.0))
        
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
    Testa o scraper otimizado do site Fusões e Aquisições.
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
