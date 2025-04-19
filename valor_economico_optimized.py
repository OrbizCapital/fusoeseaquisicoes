"""
Módulo otimizado para scraping de notícias de M&A do site Valor Econômico.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta
import logging
import re
import time
from urllib.parse import urljoin
import os
from dotenv import load_dotenv
import random
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import platform
import subprocess

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger('valor_economico_scraper')

class ValorEconomicoScraper:
    """
    Classe otimizada para scraping de notícias de M&A do site Valor Econômico.
    Requer autenticação com login e senha.
    """
    
    BASE_URL = "https://valor.globo.com"
    LOGIN_URL = "https://login.globo.com/login/438"
    SEARCH_URL = f"{BASE_URL}/busca"
    
    def __init__(self, headless=True, use_requests_fallback=True):
        """
        Inicializa o scraper com configurações para Selenium.
        
        Args:
            headless (bool): Se True, executa o navegador em modo headless.
            use_requests_fallback (bool): Se True, usa requests como fallback se Selenium falhar.
        """
        self.email = os.getenv('VALOR_EMAIL')
        self.password = os.getenv('VALOR_PASSWORD')
        self.use_requests_fallback = use_requests_fallback
        self.driver = None
        self.wait = None
        
        if not self.email or not self.password:
            logger.warning("Credenciais do Valor Econômico não encontradas nas variáveis de ambiente.")
            logger.warning("Configure VALOR_EMAIL e VALOR_PASSWORD no arquivo .env")
        
        # Inicializar sessão de requests como fallback
        if self.use_requests_fallback:
            self._init_requests_session()
        
        # Tentar inicializar Selenium
        try:
            self._init_selenium(headless)
        except Exception as e:
            logger.error(f"Erro ao inicializar Selenium: {e}")
            logger.info("Usando apenas método de fallback com requests")
            self.driver = None
            self.wait = None
    
    def _init_requests_session(self):
        """
        Inicializa sessão de requests para fallback.
        """
        # Lista de user agents para rotação
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
        ]
        
        # Inicializar sessão de requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://www.google.com/',
            'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
            'sec-ch-ua-mobile': '?0',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'cross-site',
            'sec-fetch-user': '?1',
            'dnt': '1'
        })
    
    def _init_selenium(self, headless=True):
        """
        Inicializa o driver do Selenium com configurações otimizadas.
        
        Args:
            headless (bool): Se True, executa o navegador em modo headless.
        """
        # Configuração do Selenium
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        
        # User agent para simular navegador real
        chrome_options.add_argument(f"user-agent={random.choice(self.user_agents)}")
        
        # Verificar o sistema operacional e configurar o driver adequadamente
        try:
            system = platform.system().lower()
            
            if system == 'linux':
                # Verificar se o Chrome está instalado
                try:
                    chrome_version = subprocess.check_output(['google-chrome', '--version']).decode().strip()
                    logger.info(f"Chrome instalado: {chrome_version}")
                except:
                    logger.warning("Chrome não encontrado, tentando usar ChromeDriver diretamente")
                
                # Usar caminho específico para o driver no Linux
                driver_path = ChromeDriverManager().install()
                service = Service(driver_path)
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            elif system == 'darwin':  # macOS
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            elif system == 'windows':
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            else:
                logger.error(f"Sistema operacional não suportado: {system}")
                raise Exception(f"Sistema operacional não suportado: {system}")
            
            # Timeout para espera de elementos
            self.wait = WebDriverWait(self.driver, 20)
            logger.info("Driver do Selenium inicializado com sucesso")
            
        except Exception as e:
            logger.error(f"Erro ao inicializar o driver do Selenium: {e}")
            raise
    
    def _rotate_user_agent(self):
        """
        Alterna para um novo user agent aleatório na sessão de requests.
        """
        if hasattr(self, 'session'):
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
        if not hasattr(self, 'session'):
            self._init_requests_session()
            
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
    
    def login(self):
        """
        Realiza login no site Valor Econômico.
        
        Returns:
            bool: True se o login for bem-sucedido, False caso contrário.
        """
        if not self.email or not self.password:
            logger.error("Credenciais não configuradas. Impossível fazer login.")
            return False
        
        # Se o driver não foi inicializado, tentar usar método alternativo
        if not self.driver:
            return self._login_with_requests()
        
        try:
            logger.info("Iniciando processo de login no Valor Econômico com Selenium")
            
            # Acessar página de login
            self.driver.get(self.LOGIN_URL)
            time.sleep(3)  # Aguardar carregamento da página
            
            # Preencher email
            try:
                email_field = self.wait.until(EC.presence_of_element_located((By.ID, "login")))
                email_field.clear()
                email_field.send_keys(self.email)
                
                # Clicar no botão para prosseguir
                continue_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Continuar')]")))
                continue_button.click()
                time.sleep(2)
                
                # Preencher senha
                password_field = self.wait.until(EC.presence_of_element_located((By.ID, "password")))
                password_field.clear()
                password_field.send_keys(self.password)
                
                # Clicar no botão de login
                login_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Entrar')]")))
                login_button.click()
                
                # Aguardar redirecionamento após login
                time.sleep(5)
                
                # Verificar se o login foi bem-sucedido
                if "minha-conta" in self.driver.current_url or "dashboard" in self.driver.current_url:
                    logger.info("Login realizado com sucesso via Selenium")
                    return True
                else:
                    logger.error("Falha no login via Selenium. Verifique as credenciais.")
                    return False
            
            except Exception as e:
                logger.error(f"Erro durante o login via Selenium: {e}")
                
                # Tentar método alternativo
                if self.use_requests_fallback:
                    logger.info("Tentando login alternativo via requests")
                    return self._login_with_requests()
                return False
                
        except Exception as e:
            logger.error(f"Erro durante o login via Selenium: {e}")
            
            # Tentar método alternativo
            if self.use_requests_fallback:
                logger.info("Tentando login alternativo via requests")
                return self._login_with_requests()
            return False
    
    def _login_with_requests(self):
        """
        Realiza login no site Valor Econômico usando requests.
        
        Returns:
            bool: True se o login for bem-sucedido, False caso contrário.
        """
        try:
            logger.info("Iniciando processo de login no Valor Econômico com requests")
            
            # Acessar página de login para obter cookies e tokens
            response = self._make_request(self.LOGIN_URL)
            
            if not response or response.status_code != 200:
                logger.error("Não foi possível acessar a página de login")
                return False
            
            # Extrair token CSRF ou outros parâmetros necessários
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Buscar formulário e campos ocultos
            form = soup.select_one('form')
            if not form:
                logger.error("Formulário de login não encontrado")
                return False
            
            # Extrair campos ocultos e seus valores
            hidden_fields = {}
            for hidden in form.select('input[type="hidden"]'):
                if 'name' in hidden.attrs and 'value' in hidden.attrs:
                    hidden_fields[hidden['name']] = hidden['value']
            
            # Preparar dados para login
            login_data = {
                'login': self.email,
                'password': self.password,
                **hidden_fields
            }
            
            # Obter URL de ação do formulário
            action_url = form.get('action', self.LOGIN_URL)
            if not action_url.startswith('http'):
                action_url = urljoin(self.LOGIN_URL, action_url)
            
            # Enviar requisição de login
            login_response = self.session.post(
                action_url,
                data=login_data,
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Referer': self.LOGIN_URL
                },
                allow_redirects=True
            )
            
            # Verificar se o login foi bem-sucedido
            if login_response.status_code == 200:
                # Verificar se há indicação de login bem-sucedido na resposta
                if "minha-conta" in login_response.url or "dashboard" in login_response.url:
                    logger.info("Login realizado com sucesso via requests")
                    return True
                
                # Tentar acessar uma página protegida para verificar o login
                test_response = self._make_request(f"{self.BASE_URL}/minha-conta")
                if test_response and test_response.status_code == 200:
                    logger.info("Login realizado com sucesso via requests (verificado com página protegida)")
                    return True
            
            logger.error("Falha no login via requests. Verifique as credenciais.")
            return False
            
        except Exception as e:
            logger.error(f"Erro durante o login via requests: {e}")
            return False
    
    def search_news(self, query="fusão aquisição M&A", days=30, max_pages=5):
        """
        Busca notícias relacionadas a M&A no Valor Econômico.
        
        Args:
            query (str): Termos de busca.
            days (int): Número de dias para buscar notícias (a partir de hoje).
            max_pages (int): Número máximo de páginas de resultados a serem processadas.
            
        Returns:
            list: Lista de URLs de notícias encontradas.
        """
        logger.info(f"Buscando notícias com query: '{query}', dos últimos {days} dias")
        
        # Verificar se já está logado, caso contrário fazer login
        if not self.login():
            logger.error("Não foi possível realizar a busca sem login")
            return []
        
        # Calcular a data limite
        date_limit = datetime.now() - timedelta(days=days)
        
        # Formatar a query para URL
        query_param = query.replace(" ", "+")
        
        all_news_urls = []
        
        # Tentar busca com Selenium se disponível
        if self.driver:
            try:
                return self._search_news_selenium(query, query_param, date_limit, max_pages)
            except Exception as e:
                logger.error(f"Erro na busca com Selenium: {e}")
                if not self.use_requests_fallback:
                    return []
                logger.info("Tentando busca alternativa com requests")
        
        # Busca alternativa com requests
        return self._search_news_requests(query_param, date_limit, max_pages)
    
    def _search_news_selenium(self, query, query_param, date_limit, max_pages):
        """
        Busca notícias usando Selenium.
        
        Args:
            query (str): Termos de busca originais.
            query_param (str): Termos de busca formatados para URL.
            date_limit (datetime): Data limite para filtrar notícias.
            max_pages (int): Número máximo de páginas de resultados.
            
        Returns:
            list: Lista de URLs de notícias encontradas.
        """
        all_news_urls = []
        
        # Iterar pelas páginas de resultados
        for page in range(1, max_pages + 1):
            try:
                # Construir URL de busca com parâmetros
                search_url = f"{self.SEARCH_URL}?q={query_param}&page={page}"
                
                logger.info(f"Buscando página {page} de resultados: {search_url}")
                self.driver.get(search_url)
                time.sleep(3)  # Aguardar carregamento dos resultados
                
                # Extrair resultados da página
                soup = BeautifulSoup(self.driver.page_source, 'lxml')
                
                # Encontrar todos os resultados de notícias
                news_items = soup.select('.c-card')
                
                if not news_items:
                    logger.info(f"Nenhum resultado encontrado na página {page}")
                    break
                
                # Processar cada item de notícia
                pub_date = None
                for item in news_items:
                    # Extrair data da publicação
                    date_elem = item.select_one('.c-card__info time')
                    if not date_elem:
                        continue
                    
                    # Converter texto de data para objeto datetime
                    try:
                        # Formato típico: "DD/MM/AAAA HH:MM"
                        date_text = date_elem.text.strip()
                        date_part = re.search(r'(\d{2}/\d{2}/\d{4})', date_text)
                        
                        if date_part:
                            pub_date = datetime.strptime(date_part.group(1), '%d/%m/%Y')
                            
                            # Verificar se a notícia está dentro do período desejado
                            if pub_date < date_limit:
                                logger.info(f"Atingida data limite ({date_limit.strftime('%d/%m/%Y')})")
                                break
                            
                            # Extrair URL da notícia
                            link_elem = item.select_one('a.c-card__link')
                            if link_elem and 'href' in link_elem.attrs:
                                news_url = urljoin(self.BASE_URL, link_elem['href'])
                                all_news_urls.append(news_url)
                                logger.debug(f"Adicionada notícia: {news_url}")
                    except Exception as e:
                        logger.error(f"Erro ao processar data da notícia: {e}")
                
                # Se chegamos à data limite, paramos a busca
                if pub_date and pub_date < date_limit:
                    break
                
                # Pausa para não sobrecarregar o servidor
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Erro ao buscar página {page}: {e}")
                break
        
        logger.info(f"Total de {len(all_news_urls)} notícias encontradas via Selenium")
        return all_news_urls
    
    def _search_news_requests(self, query_param, date_limit, max_pages):
        """
        Busca notícias usando requests.
        
        Args:
            query_param (str): Termos de busca formatados para URL.
            date_limit (datetime): Data limite para filtrar notícias.
            max_pages (int): Número máximo de páginas de resultados.
            
        Returns:
            list: Lista de URLs de notícias encontradas.
        """
        all_news_urls = []
        
        # Iterar pelas páginas de resultados
        for page in range(1, max_pages + 1):
            try:
                # Construir URL de busca com parâmetros
                search_url = f"{self.SEARCH_URL}?q={query_param}&page={page}"
                
                logger.info(f"Buscando página {page} de resultados: {search_url}")
                response = self._make_request(search_url)
                
                if not response or response.status_code != 200:
                    logger.error(f"Erro ao acessar página {page}: {response.status_code if response else 'Sem resposta'}")
                    break
                
                # Extrair resultados da página
                soup = BeautifulSoup(response.text, 'lxml')
                
                # Encontrar todos os resultados de notícias
                news_items = soup.select('.c-card')
                
                if not news_items:
                    logger.info(f"Nenhum resultado encontrado na página {page}")
                    break
                
                # Processar cada item de notícia
                pub_date = None
                for item in news_items:
                    # Extrair data da publicação
                    date_elem = item.select_one('.c-card__info time')
                    if not date_elem:
                        continue
                    
                    # Converter texto de data para objeto datetime
                    try:
                        # Formato típico: "DD/MM/AAAA HH:MM"
                        date_text = date_elem.text.strip()
                        date_part = re.search(r'(\d{2}/\d{2}/\d{4})', date_text)
                        
                        if date_part:
                            pub_date = datetime.strptime(date_part.group(1), '%d/%m/%Y')
                            
                            # Verificar se a notícia está dentro do período desejado
                            if pub_date < date_limit:
                                logger.info(f"Atingida data limite ({date_limit.strftime('%d/%m/%Y')})")
                                break
                            
                            # Extrair URL da notícia
                            link_elem = item.select_one('a.c-card__link')
                            if link_elem and 'href' in link_elem.attrs:
                                news_url = urljoin(self.BASE_URL, link_elem['href'])
                                all_news_urls.append(news_url)
                                logger.debug(f"Adicionada notícia: {news_url}")
                    except Exception as e:
                        logger.error(f"Erro ao processar data da notícia: {e}")
                
                # Se chegamos à data limite, paramos a busca
                if pub_date and pub_date < date_limit:
                    break
                
                # Pausa para não sobrecarregar o servidor
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Erro ao buscar página {page}: {e}")
                break
        
        logger.info(f"Total de {len(all_news_urls)} notícias encontradas via requests")
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
        
        # Tentar extração com Selenium se disponível
        if self.driver:
            try:
                return self._extract_news_data_selenium(url)
            except Exception as e:
                logger.error(f"Erro ao extrair dados da notícia com Selenium: {e}")
                if not self.use_requests_fallback:
                    return None
                logger.info("Tentando extração alternativa com requests")
        
        # Extração alternativa com requests
        return self._extract_news_data_requests(url)
    
    def _extract_news_data_selenium(self, url):
        """
        Extrai informações detalhadas de uma notícia usando Selenium.
        
        Args:
            url (str): URL da notícia.
            
        Returns:
            dict: Dicionário com dados extraídos ou None se falhar.
        """
        try:
            # Acessar a página da notícia
            self.driver.get(url)
            time.sleep(3)  # Aguardar carregamento da página
            
            # Verificar se há paywall e se estamos logados
            if "Para continuar lendo" in self.driver.page_source and "Faça login ou assine" in self.driver.page_source:
                logger.warning("Detectado paywall. Tentando fazer login novamente.")
                if not self.login():
                    logger.error("Não foi possível fazer login para acessar o conteúdo completo.")
                    return None
                
                # Acessar a página novamente após login
                self.driver.get(url)
                time.sleep(3)
            
            # Extrair o conteúdo da página
            soup = BeautifulSoup(self.driver.page_source, 'lxml')
            
            # Extrair título
            title = soup.select_one('h1.content-head__title')
            title_text = title.text.strip() if title else "Título não encontrado"
            
            # Extrair data
            date_elem = soup.select_one('.content-publication-data__updated')
            date_text = date_elem.text.strip() if date_elem else "Data não encontrada"
            
            # Extrair conteúdo completo
            content_elems = soup.select('.content-text p')
            content_text = ' '.join([p.text.strip() for p in content_elems]) if content_elems else ""
            
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
                # Extrair dados usando expressões regulares
                transaction_data = self._extract_transaction_data(transaction_data, content_text)
            
            return transaction_data
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados da notícia {url} com Selenium: {e}")
            return None
    
    def _extract_news_data_requests(self, url):
        """
        Extrai informações detalhadas de uma notícia usando requests.
        
        Args:
            url (str): URL da notícia.
            
        Returns:
            dict: Dicionário com dados extraídos ou None se falhar.
        """
        try:
            # Acessar a página da notícia
            response = self._make_request(url)
            
            if not response or response.status_code != 200:
                logger.error(f"Não foi possível acessar a notícia: {url}")
                return None
            
            # Verificar se há paywall
            if "Para continuar lendo" in response.text and "Faça login ou assine" in response.text:
                logger.warning("Detectado paywall. Tentando fazer login novamente.")
                if not self._login_with_requests():
                    logger.error("Não foi possível fazer login para acessar o conteúdo completo.")
                    return None
                
                # Acessar a página novamente após login
                response = self._make_request(url)
                
                if not response or response.status_code != 200:
                    logger.error(f"Não foi possível acessar a notícia após login: {url}")
                    return None
            
            # Extrair o conteúdo da página
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Extrair título
            title = soup.select_one('h1.content-head__title')
            title_text = title.text.strip() if title else "Título não encontrado"
            
            # Extrair data
            date_elem = soup.select_one('.content-publication-data__updated')
            date_text = date_elem.text.strip() if date_elem else "Data não encontrada"
            
            # Extrair conteúdo completo
            content_elems = soup.select('.content-text p')
            content_text = ' '.join([p.text.strip() for p in content_elems]) if content_elems else ""
            
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
                # Extrair dados usando expressões regulares
                transaction_data = self._extract_transaction_data(transaction_data, content_text)
            
            return transaction_data
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados da notícia {url} com requests: {e}")
            return None
    
    def _extract_transaction_data(self, transaction_data, content_text):
        """
        Extrai dados de transação do conteúdo da notícia.
        
        Args:
            transaction_data (dict): Dicionário inicial com dados da transação.
            content_text (str): Texto do conteúdo da notícia.
            
        Returns:
            dict: Dicionário atualizado com dados extraídos.
        """
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
        try:
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
                
        except Exception as e:
            logger.error(f"Erro na execução do scraper: {e}")
            return pd.DataFrame()
        
        finally:
            # Fechar o driver ao finalizar
            self.close()
    
    def close(self):
        """
        Fecha o driver do Selenium.
        """
        if hasattr(self, 'driver') and self.driver:
            try:
                self.driver.quit()
                logger.info("Driver do Selenium fechado.")
            except Exception as e:
                logger.error(f"Erro ao fechar driver do Selenium: {e}")

# Função para teste
def test_valor_economico_scraper():
    """
    Testa o scraper otimizado do Valor Econômico.
    """
    # Verificar se as credenciais estão configuradas
    if not os.getenv('VALOR_EMAIL') or not os.getenv('VALOR_PASSWORD'):
        print("Credenciais não configuradas. Configure as variáveis de ambiente VALOR_EMAIL e VALOR_PASSWORD.")
        return
    
    scraper = ValorEconomicoScraper(headless=True, use_requests_fallback=True)
    try:
        df = scraper.run_scraper(days=30, max_pages=2)
        
        if not df.empty:
            print(f"Dados extraídos: {len(df)} notícias")
            print(df.head())
            
            # Salvar para teste
            df.to_excel("valor_economico_test.xlsx", index=False)
            print("Dados salvos em 'valor_economico_test.xlsx'")
        else:
            print("Nenhum dado extraído")
    finally:
        scraper.close()

if __name__ == "__main__":
    test_valor_economico_scraper()
