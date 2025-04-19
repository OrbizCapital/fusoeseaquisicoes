# Serviço de Notícias de M&A Brasil

Este projeto implementa um serviço automatizado para coleta, processamento e apresentação de notícias de fusões e aquisições (M&A) do Brasil, extraídas de três fontes principais:

1. Pipeline Valor
2. Valor Econômico
3. Fusões e Aquisições

O sistema realiza buscas periódicas, extrai informações estruturadas sobre as transações (comprador, empresa adquirida, valor, múltiplo) e disponibiliza os resultados em formato Excel através de uma interface web.

## Funcionalidades

- **Extração automática**: Coleta diária de notícias de M&A de múltiplas fontes
- **Processamento inteligente**: Extração de dados estruturados sobre as transações
- **Interface web**: Acesso fácil aos relatórios e configuração do sistema
- **Agendamento flexível**: Configuração de horário para execução automática
- **Exportação em Excel**: Relatórios organizados e prontos para uso
- **Histórico completo**: Registro de todas as extrações realizadas

## Requisitos

- Python 3.8 ou superior
- Pacotes Python listados em `requirements.txt`
- Credenciais de acesso para o Valor Econômico (opcional)
- Conexão com a internet

## Instalação

1. Clone o repositório ou descompacte o arquivo do projeto
2. Instale as dependências:

```bash
pip install -r requirements.txt
```

3. Configure as credenciais para o Valor Econômico (opcional):
   - Crie um arquivo `.env` na raiz do projeto
   - Adicione as seguintes linhas:
   ```
   VALOR_EMAIL=seu_email@exemplo.com
   VALOR_PASSWORD=sua_senha
   ```

## Execução

### Iniciar o serviço web

```bash
cd /caminho/para/ma_scraper
python app.py
```

O serviço estará disponível em http://localhost:5000

### Executar extração manualmente via linha de comando

```bash
cd /caminho/para/ma_scraper
python -c "from ma_extractor_optimized import MAExtractorOptimized; extractor = MAExtractorOptimized(); extractor.run_extraction()"
```

## Estrutura do Projeto

```
ma_scraper/
├── app.py                      # Aplicação web Flask
├── ma_extractor.py             # Extrator original de dados
├── ma_extractor_optimized.py   # Extrator otimizado com maior confiabilidade
├── requirements.txt            # Dependências do projeto
├── config.json                 # Configurações do sistema
├── .env                        # Credenciais (não versionado)
├── scrapers/                   # Módulos de scraping para cada fonte
│   ├── pipeline_valor.py       # Scraper para Pipeline Valor
│   ├── valor_economico.py      # Scraper original para Valor Econômico
│   ├── valor_economico_optimized.py  # Scraper otimizado para Valor Econômico
│   ├── fusoes_aquisicoes.py    # Scraper original para Fusões e Aquisições
│   └── fusoes_aquisicoes_optimized.py  # Scraper otimizado para Fusões e Aquisições
├── templates/                  # Templates HTML para interface web
│   └── index.html              # Página principal da interface
└── output/                     # Diretório para arquivos de saída
    └── extraction_history.json # Histórico de extrações
```

## Uso da Interface Web

### Página Inicial - Relatórios

Exibe todos os relatórios disponíveis para download, com informações sobre data de geração, tamanho e número de notícias.

### Nova Extração

Permite iniciar uma extração manual com parâmetros personalizados:
- **Termos de Busca**: Palavras-chave para buscar notícias relacionadas a M&A
- **Período (dias)**: Número de dias para buscar notícias (a partir de hoje)
- **Páginas por Site**: Número máximo de páginas a serem processadas em cada site
- **Executar em paralelo**: Opção para executar os scrapers simultaneamente

### Histórico

Mostra o histórico completo de extrações realizadas, incluindo:
- Data e hora da extração
- Duração do processo
- Número de notícias encontradas
- Parâmetros utilizados
- Link para download do relatório

### Configurações

#### Configurações Gerais
- Termos de busca padrão
- Período padrão (dias)
- Páginas padrão
- Opção de execução em paralelo
- Horário de execução diária
- Ativação/desativação da execução automática

#### Credenciais do Valor Econômico
- Email de acesso
- Senha

## Detalhes Técnicos

### Scrapers

O sistema utiliza diferentes técnicas de web scraping para cada fonte:

- **Pipeline Valor**: Scraping baseado em requests com BeautifulSoup
- **Valor Econômico**: Combinação de Selenium para autenticação e navegação com fallback para requests
- **Fusões e Aquisições**: Scraping otimizado com rotação de user agents e métodos alternativos de busca

### Extração de Dados

O sistema extrai automaticamente as seguintes informações das notícias:

- **Comprador**: Empresa que realizou a aquisição
- **Empresa Comprada**: Empresa que foi adquirida
- **Valor da Transação**: Montante financeiro envolvido na transação
- **Múltiplo**: Relação entre o valor da transação e algum indicador financeiro (ex: EBITDA)

### Otimizações

O sistema inclui várias otimizações para aumentar a confiabilidade:

- Rotação de user agents para evitar bloqueios
- Retry com backoff exponencial para lidar com falhas temporárias
- Métodos alternativos de busca quando o acesso direto falha
- Fallback de Selenium para requests quando necessário
- Processamento paralelo para maior velocidade (opcional)

## Solução de Problemas

### Erro de acesso ao Valor Econômico

Se encontrar problemas de acesso ao Valor Econômico, verifique:
1. Se as credenciais no arquivo `.env` estão corretas
2. Se sua assinatura do Valor Econômico está ativa
3. Se não há bloqueio por excesso de requisições (aguarde alguns minutos e tente novamente)

### Erro 403 no site Fusões e Aquisições

O site Fusões e Aquisições pode bloquear requisições automatizadas. O sistema implementa:
1. Rotação de user agents
2. Delays aleatórios entre requisições
3. Métodos alternativos de busca

Se os problemas persistirem, aguarde algumas horas antes de tentar novamente.

### Problemas com o ChromeDriver

Se encontrar erros relacionados ao ChromeDriver:
1. Verifique se o Google Chrome está instalado no sistema
2. O sistema tentará usar métodos alternativos automaticamente

## Limitações

- A extração de dados depende da estrutura atual dos sites, mudanças nos layouts podem afetar o funcionamento
- O acesso ao conteúdo completo do Valor Econômico requer uma assinatura válida
- Alguns sites podem implementar medidas anti-scraping que limitam o acesso automatizado
- A precisão da extração de dados estruturados (comprador, empresa adquirida, valor, múltiplo) depende do formato e clareza das notícias originais

## Manutenção

Para manter o sistema funcionando corretamente:

1. Verifique periodicamente se as credenciais do Valor Econômico estão válidas
2. Monitore o log de extrações para identificar possíveis problemas
3. Atualize as dependências quando necessário
4. Verifique se há mudanças nos layouts dos sites que possam afetar a extração

## Próximos Passos

Possíveis melhorias futuras para o sistema:

1. Adicionar mais fontes de notícias de M&A
2. Implementar análise de sentimento das notícias
3. Criar visualizações gráficas dos dados de transações
4. Adicionar notificações por email para novas transações importantes
5. Melhorar a precisão da extração de dados estruturados com técnicas de NLP

## Suporte

Para suporte ou dúvidas sobre o sistema, entre em contato com o desenvolvedor.
