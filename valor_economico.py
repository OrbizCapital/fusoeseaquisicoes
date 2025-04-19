"""
Módulo para scraping de notícias de M&A do site Valor Econômico.
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
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

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
    Classe para scraping de notícias de M&A do site Valor Econômico.
    Requer autenticação com login e senha.
    """
    
    BASE_URL = "https://valor.globo.com"
    LOGIN_URL = "https://login.globo.com/login/438"
    SEARCH_URL = f"{BASE_URL}/busca"
    
    def __init__(self, headless=True):
        """
        Inicializa o scraper com configurações para Selenium.
        
        Args:
            headless (bool): Se True, executa o navegador em modo headless.
        """
        self.email = os.getenv('VALOR_EMAIL')
        self.password = os.getenv('VALOR_PASSWORD')
        
        if not self.email or not self.password:
            logger.warning("Credenciais do Valor Econômico não encontradas nas variáveis de ambiente.")
            logger.warning("Configure VALOR_EMAIL e VALOR_PASSWORD no arquivo .env")
        
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
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        # Inicializar o driver
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        
        # Timeout para espera de elementos
        self.wait = WebDriverWait(self.driver, 20)
    
    def login(self):
        """
        Realiza login no site Valor Econômico.
        
        Returns:
            bool: True se o login for bem-sucedido, False caso contrário.
        """
        if not self.email or not self.password:
            logger.error("Credenciais não configuradas. Impossível fazer login.")
            return False
        
        try:
            logger.info("Iniciando processo de login no Valor Econômico")
            
            # Acessar página de login
            self.driver.get(self.LOGIN_URL)
            time.sleep(3)  # Aguardar carregamento da página
            
            # Preencher email
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
                logger.info("Login realizado com sucesso")
                return True
            else:
                logger.error("Falha no login. Verifique as credenciais.")
                return False
                
        except Exception as e:
            logger.error(f"Erro durante o login: {e}")
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
                time.sleep(3)
            
            # Criar DataFrame
            if all_data:
                df = pd.DataFrame(all_data)
                logger.info(f"Extração concluída. {len(df)} notícias processadas.")
                return df
            else:
                logger.warning("Nenhum dado extraído.")
                return pd.DataFrame()
                
        finally:
            # Fechar o driver ao finalizar
            self.close()
    
    def close(self):
        """
        Fecha o driver do Selenium.
        """
        if hasattr(self, 'driver'):
            self.driver.quit()
            logger.info("Driver do Selenium fechado.")

# Função para teste
def test_valor_economico_scraper():
    """
    Testa o scraper do Valor Econômico.
    """
    # Verificar se as credenciais estão configuradas
    if not os.getenv('VALOR_EMAIL') or not os.getenv('VALOR_PASSWORD'):
        print("Credenciais não configuradas. Configure as variáveis de ambiente VALOR_EMAIL e VALOR_PASSWORD.")
        return
    
    scraper = ValorEconomicoScraper(headless=True)
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
