"""
Aplicação web para serviço de scraping de notícias de M&A.
"""

from flask import Flask, render_template, request, send_file, jsonify
import os
import schedule
import threading
import time
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
import logging
import json

# Importar o extrator de dados
from ma_extractor import MAExtractor

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('ma_scraper_app')

# Inicializar aplicação Flask
app = Flask(__name__)

# Configurações
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
HISTORY_FILE = os.path.join(OUTPUT_DIR, "extraction_history.json")
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

# Criar diretório de saída se não existir
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    logger.info(f"Diretório de saída criado: {OUTPUT_DIR}")

# Inicializar extrator
extractor = MAExtractor(output_dir=OUTPUT_DIR)

# Variáveis globais
last_extraction_time = None
last_extraction_file = None
extraction_in_progress = False
extraction_history = []

def load_config():
    """
    Carrega configurações do arquivo config.json.
    
    Returns:
        dict: Configurações carregadas.
    """
    default_config = {
        "query": "fusão aquisição M&A",
        "days": 30,
        "max_pages": 5,
        "parallel": True,
        "schedule_time": "04:00",  # 4 AM
        "enabled": True
    }
    
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info("Configurações carregadas do arquivo")
            return config
        else:
            # Criar arquivo de configuração padrão
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=4)
            logger.info("Arquivo de configuração padrão criado")
            return default_config
    except Exception as e:
        logger.error(f"Erro ao carregar configurações: {e}")
        return default_config

def save_config(config):
    """
    Salva configurações no arquivo config.json.
    
    Args:
        config (dict): Configurações a serem salvas.
    """
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4)
        logger.info("Configurações salvas no arquivo")
    except Exception as e:
        logger.error(f"Erro ao salvar configurações: {e}")

def load_extraction_history():
    """
    Carrega histórico de extrações do arquivo.
    """
    global extraction_history
    
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                extraction_history = json.load(f)
            logger.info(f"Histórico de extrações carregado: {len(extraction_history)} registros")
        else:
            extraction_history = []
            logger.info("Nenhum histórico de extrações encontrado")
    except Exception as e:
        logger.error(f"Erro ao carregar histórico de extrações: {e}")
        extraction_history = []

def save_extraction_history():
    """
    Salva histórico de extrações no arquivo.
    """
    try:
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(extraction_history, f, indent=4)
        logger.info("Histórico de extrações salvo")
    except Exception as e:
        logger.error(f"Erro ao salvar histórico de extrações: {e}")

def run_scheduled_extraction():
    """
    Executa extração agendada.
    """
    global extraction_in_progress, last_extraction_time, last_extraction_file, extraction_history
    
    if extraction_in_progress:
        logger.warning("Extração já em andamento. Agendamento ignorado.")
        return
    
    logger.info("Iniciando extração agendada")
    extraction_in_progress = True
    
    try:
        config = load_config()
        
        # Verificar se o agendamento está habilitado
        if not config.get("enabled", True):
            logger.info("Agendamento desabilitado nas configurações")
            extraction_in_progress = False
            return
        
        # Executar extração
        start_time = datetime.now()
        file_path = extractor.run_extraction(
            query=config.get("query", "fusão aquisição M&A"),
            days=config.get("days", 30),
            max_pages=config.get("max_pages", 5),
            parallel=config.get("parallel", True)
        )
        end_time = datetime.now()
        
        if file_path:
            # Atualizar variáveis globais
            last_extraction_time = start_time
            last_extraction_file = os.path.basename(file_path)
            
            # Adicionar ao histórico
            extraction_record = {
                "timestamp": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "duration": (end_time - start_time).total_seconds(),
                "file": os.path.basename(file_path),
                "query": config.get("query", "fusão aquisição M&A"),
                "days": config.get("days", 30)
            }
            
            # Carregar o arquivo Excel para contar o número de notícias
            try:
                df = pd.read_excel(file_path)
                extraction_record["news_count"] = len(df)
            except:
                extraction_record["news_count"] = 0
            
            extraction_history.append(extraction_record)
            save_extraction_history()
            
            logger.info(f"Extração agendada concluída. Arquivo salvo: {file_path}")
        else:
            logger.error("Falha na extração agendada")
    
    except Exception as e:
        logger.error(f"Erro na extração agendada: {e}")
    
    finally:
        extraction_in_progress = False

def schedule_thread():
    """
    Thread para executar agendamentos.
    """
    while True:
        schedule.run_pending()
        time.sleep(60)  # Verificar a cada minuto

def setup_schedule():
    """
    Configura o agendamento de extrações.
    """
    config = load_config()
    schedule_time = config.get("schedule_time", "04:00")
    
    # Limpar agendamentos existentes
    schedule.clear()
    
    # Configurar novo agendamento
    schedule.every().day.at(schedule_time).do(run_scheduled_extraction)
    logger.info(f"Extração agendada para todos os dias às {schedule_time}")
    
    # Iniciar thread de agendamento
    threading.Thread(target=schedule_thread, daemon=True).start()

# Rotas da aplicação web
@app.route('/')
def index():
    """
    Página inicial da aplicação.
    """
    # Carregar histórico de extrações
    load_extraction_history()
    
    # Obter lista de arquivos disponíveis
    files = []
    try:
        for file in os.listdir(OUTPUT_DIR):
            if file.endswith('.xlsx'):
                file_path = os.path.join(OUTPUT_DIR, file)
                file_stats = os.stat(file_path)
                file_size = file_stats.st_size / (1024 * 1024)  # Tamanho em MB
                file_date = datetime.fromtimestamp(file_stats.st_mtime)
                
                # Obter contagem de notícias
                try:
                    df = pd.read_excel(file_path)
                    news_count = len(df)
                except:
                    news_count = 0
                
                files.append({
                    'name': file,
                    'size': f"{file_size:.2f} MB",
                    'date': file_date.strftime("%Y-%m-%d %H:%M:%S"),
                    'news_count': news_count
                })
        
        # Ordenar por data (mais recentes primeiro)
        files.sort(key=lambda x: x['date'], reverse=True)
    except Exception as e:
        logger.error(f"Erro ao listar arquivos: {e}")
    
    # Carregar configurações
    config = load_config()
    
    return render_template(
        'index.html',
        files=files,
        history=extraction_history,
        config=config,
        extraction_in_progress=extraction_in_progress,
        last_extraction_time=last_extraction_time
    )

@app.route('/download/<filename>')
def download_file(filename):
    """
    Download de arquivo.
    """
    try:
        file_path = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(file_path) and filename.endswith('.xlsx'):
            return send_file(file_path, as_attachment=True)
        else:
            return "Arquivo não encontrado", 404
    except Exception as e:
        logger.error(f"Erro ao fazer download do arquivo: {e}")
        return "Erro ao fazer download", 500

@app.route('/run-extraction', methods=['POST'])
def run_extraction():
    """
    Executa extração manual.
    """
    global extraction_in_progress, last_extraction_time, last_extraction_file, extraction_history
    
    if extraction_in_progress:
        return jsonify({"status": "error", "message": "Extração já em andamento"})
    
    extraction_in_progress = True
    
    try:
        # Obter parâmetros do formulário
        query = request.form.get('query', "fusão aquisição M&A")
        days = int(request.form.get('days', 30))
        max_pages = int(request.form.get('max_pages', 5))
        parallel = request.form.get('parallel', 'true') == 'true'
        
        # Executar extração em thread separada para não bloquear a aplicação
        def extraction_thread():
            global extraction_in_progress, last_extraction_time, last_extraction_file, extraction_history
            
            try:
                start_time = datetime.now()
                file_path = extractor.run_extraction(
                    query=query,
                    days=days,
                    max_pages=max_pages,
                    parallel=parallel
                )
                end_time = datetime.now()
                
                if file_path:
                    # Atualizar variáveis globais
                    last_extraction_time = start_time
                    last_extraction_file = os.path.basename(file_path)
                    
                    # Adicionar ao histórico
                    extraction_record = {
                        "timestamp": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "duration": (end_time - start_time).total_seconds(),
                        "file": os.path.basename(file_path),
                        "query": query,
                        "days": days
                    }
                    
                    # Carregar o arquivo Excel para contar o número de notícias
                    try:
                        df = pd.read_excel(file_path)
                        extraction_record["news_count"] = len(df)
                    except:
                        extraction_record["news_count"] = 0
                    
                    extraction_history.append(extraction_record)
                    save_extraction_history()
                    
                    logger.info(f"Extração manual concluída. Arquivo salvo: {file_path}")
            
            except Exception as e:
                logger.error(f"Erro na extração manual: {e}")
            
            finally:
                extraction_in_progress = False
        
        # Iniciar thread de extração
        threading.Thread(target=extraction_thread).start()
        
        return jsonify({"status": "success", "message": "Extração iniciada"})
    
    except Exception as e:
        extraction_in_progress = False
        logger.error(f"Erro ao iniciar extração: {e}")
        return jsonify({"status": "error", "message": str(e)})

@app.route('/status')
def get_status():
    """
    Retorna o status atual da extração.
    """
    return jsonify({
        "extraction_in_progress": extraction_in_progress,
        "last_extraction_time": last_extraction_time.strftime("%Y-%m-%d %H:%M:%S") if last_extraction_time else None,
        "last_extraction_file": last_extraction_file
    })

@app.route('/save-config', methods=['POST'])
def save_app_config():
    """
    Salva configurações da aplicação.
    """
    try:
        # Obter parâmetros do formulário
        new_config = {
            "query": request.form.get('query', "fusão aquisição M&A"),
            "days": int(request.form.get('days', 30)),
            "max_pages": int(request.form.get('max_pages', 5)),
            "parallel": request.form.get('parallel', 'true') == 'true',
            "schedule_time": request.form.get('schedule_time', "04:00"),
            "enabled": request.form.get('enabled', 'true') == 'true'
        }
        
        # Salvar configurações
        save_config(new_config)
        
        # Reconfigurar agendamento
        setup_schedule()
        
        return jsonify({"status": "success", "message": "Configurações salvas"})
    
    except Exception as e:
        logger.error(f"Erro ao salvar configurações: {e}")
        return jsonify({"status": "error", "message": str(e)})

@app.route('/setup-credentials', methods=['POST'])
def setup_credentials():
    """
    Configura credenciais para o Valor Econômico.
    """
    try:
        email = request.form.get('email', '')
        password = request.form.get('password', '')
        
        if not email or not password:
            return jsonify({"status": "error", "message": "Email e senha são obrigatórios"})
        
        # Atualizar arquivo .env
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        
        # Verificar se o arquivo já existe
        if os.path.exists(env_path):
            # Ler conteúdo atual
            with open(env_path, 'r') as f:
                lines = f.readlines()
            
            # Atualizar ou adicionar variáveis
            email_found = False
            password_found = False
            
            for i, line in enumerate(lines):
                if line.startswith('VALOR_EMAIL='):
                    lines[i] = f'VALOR_EMAIL={email}\n'
                    email_found = True
                elif line.startswith('VALOR_PASSWORD='):
                    lines[i] = f'VALOR_PASSWORD={password}\n'
                    password_found = True
            
            # Adicionar variáveis se não encontradas
            if not email_found:
                lines.append(f'VALOR_EMAIL={email}\n')
            if not password_found:
                lines.append(f'VALOR_PASSWORD={password}\n')
            
            # Escrever de volta ao arquivo
            with open(env_path, 'w') as f:
                f.writelines(lines)
        else:
            # Criar novo arquivo
            with open(env_path, 'w') as f:
                f.write(f'VALOR_EMAIL={email}\n')
                f.write(f'VALOR_PASSWORD={password}\n')
        
        # Recarregar variáveis de ambiente
        load_dotenv(override=True)
        
        return jsonify({"status": "success", "message": "Credenciais salvas"})
    
    except Exception as e:
        logger.error(f"Erro ao configurar credenciais: {e}")
        return jsonify({"status": "error", "message": str(e)})

# Inicialização da aplicação
def init_app():
    """
    Inicializa a aplicação.
    """
    # Carregar histórico de extrações
    load_extraction_history()
    
    # Configurar agendamento
    setup_schedule()
    
    logger.info("Aplicação inicializada")

# Inicializar a aplicação
init_app()

if __name__ == '__main__':
    # Iniciar aplicação Flask
    app.run(host='0.0.0.0', port=5000, debug=True)
