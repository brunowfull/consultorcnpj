import requests
import re
import json
import time
import sqlite3
import logging
import csv
import random
import os
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
from datetime import datetime
from typing import Dict, Any, Optional, List
from logging.handlers import RotatingFileHandler
import threading # For running batch process without freezing GUI

# --- Configurações globais ---
TOKEN = "f7f2086d9fb73432a081a843ab6555d51ef8d4382098b0144182c80bceef3ab4"
CLEARBIT_API_KEY = "sk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"  # Substitua pela sua chave Clearbit
SOCIAL_SEARCHER_KEY = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"  # Substitua pela sua chave Social Searcher
CACHE_DAYS = 30
MAX_API_REQUESTS = 3
API_REQUEST_WINDOW = 60  # segundos
BATCH_DELAY_BETWEEN_REQUESTS = 1 # Segundos entre requisições em lote para ser gentil com a API

# --- Configuração de logging ---
def setup_logger():
    logger = logging.getLogger('cnpj_consultor')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # Handler para arquivo (rotação diária)
    file_handler = RotatingFileHandler(
        'cnpj_consultor.log', 
        maxBytes=10*1024*1024,  # 10 MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

logger = setup_logger()

# --- Monitor de API ---
class APIMonitor:
    def __init__(self, max_requests=MAX_API_REQUESTS, per_seconds=API_REQUEST_WINDOW):
        self.max_requests = max_requests
        self.per_seconds = per_seconds
        self.requests = []

    def can_make_request(self):
        now = time.time()
        # Remove requests mais antigas que o período
        self.requests = [t for t in self.requests if now - t < self.per_seconds]
        return len(self.requests) < self.max_requests

    def record_request(self):
        self.requests.append(time.time())

    def wait_if_needed(self):
        while not self.can_make_request():
            oldest = min(self.requests)
            wait_time = self.per_seconds - (time.time() - oldest) + 0.1
            logger.info(f"Atingido limite de requisições. Aguardando {wait_time:.1f} segundos...")
            time.sleep(wait_time)

# Inicializar o monitor de API
api_monitor = APIMonitor()

# --- Configuração de cache ---
def setup_cache_db():
    conn = sqlite3.connect('cnpj_cache.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS consultas
                 (cnpj TEXT PRIMARY KEY, data TEXT, resultado TEXT)''')
    conn.commit()
    conn.close()

def salvar_consulta_cache(cnpj: str, resultado: dict):
    conn = sqlite3.connect('cnpj_cache.db')
    c = conn.cursor()
    c.execute("REPLACE INTO consultas VALUES (?, ?, ?)",
              (cnpj, datetime.now().isoformat(), json.dumps(resultado)))
    conn.commit()
    conn.close()

def buscar_cache(cnpj: str) -> Optional[dict]:
    conn = sqlite3.connect('cnpj_cache.db')
    c = conn.cursor()
    c.execute("SELECT resultado FROM consultas WHERE cnpj = ?", (cnpj,))
    row = c.fetchone()
    conn.close()
    if row:
        data = json.loads(row[0])
        # Verifica se o cache ainda é válido
        cache_date_str = data.get('cache_date', '2000-01-01T00:00:00')
        try:
            # Tentar parse com segundos e microssegundos
            cache_date = datetime.fromisoformat(cache_date_str.replace('Z', '+00:00'))
        except ValueError:
            try:
                # Fallback para formato sem microssegundos
                cache_date = datetime.fromisoformat(cache_date_str.split('.')[0])
            except ValueError:
                # Se falhar, considerar muito antigo
                cache_date = datetime.min

        if (datetime.now() - cache_date).days <= CACHE_DAYS:
            logger.debug(f"Cache válido encontrado para CNPJ: {cnpj}")
            return data
        else:
            logger.debug(f"Cache expirado para CNPJ: {cnpj}")
    return None

# --- Funções auxiliares ---
def limpar_cnpj(cnpj: str) -> str:
    """Remove todos os caracteres não numéricos do CNPJ"""
    return re.sub(r'\D', '', cnpj)

def validar_digitos_cnpj(cnpj: str) -> bool:
    """Valida os dígitos verificadores do CNPJ"""
    cnpj_limpo = limpar_cnpj(cnpj)
    if len(cnpj_limpo) != 14:
        return False
    # Verifica se todos os dígitos são iguais
    if cnpj_limpo == cnpj_limpo[0] * 14:
        return False
    # Cálculo do primeiro dígito verificador
    soma = 0
    peso = 5
    for i in range(12):
        soma += int(cnpj_limpo[i]) * peso
        peso = 9 if peso == 2 else peso - 1
    resto = soma % 11
    digito1 = 0 if resto < 2 else 11 - resto
    # Cálculo do segundo dígito verificador
    soma = 0
    peso = 6
    for i in range(13):
        soma += int(cnpj_limpo[i]) * peso
        peso = 9 if peso == 2 else peso - 1
    resto = soma % 11
    digito2 = 0 if resto < 2 else 11 - resto
    return int(cnpj_limpo[12]) == digito1 and int(cnpj_limpo[13]) == digito2

def validar_cnpj(cnpj: str) -> bool:
    """Valida o formato e dígitos verificadores do CNPJ"""
    cnpj_limpo = limpar_cnpj(cnpj)
    return len(cnpj_limpo) == 14 and cnpj_limpo.isdigit() and validar_digitos_cnpj(cnpj_limpo)

def formatar_endereco(endereco_data: Dict[str, Any]) -> str:
    """Formata os dados de endereço em uma string legível"""
    parts = [
        endereco_data.get('logradouro', ''),
        endereco_data.get('numero', ''),
        endereco_data.get('complemento', ''),
        endereco_data.get('bairro', ''),
        endereco_data.get('municipio', ''),
        endereco_data.get('uf', ''),
        endereco_data.get('cep', '')
    ]
    # Filtra partes vazias e junta com vírgulas
    return ', '.join(filter(None, parts))

def formatar_atividades(atividades: list) -> list:
    """Formata as atividades em uma lista mais legível"""
    return [
        {
            'Código': ativ.get('code', ''),
            'Descrição': ativ.get('text', '')
        }
        for ativ in atividades
    ]

def formatar_socios(socios: list) -> list:
    """Formata os dados dos sócios"""
    return [
        {
            'Nome': socio.get('nome', ''),
            'Qualificação': socio.get('qual', ''),
            'País': socio.get('pais', ''),
            'Documento': socio.get('doc', '')
        }
        for socio in socios
    ]

def formatar_speedio(data: Dict[str, Any]) -> Dict[str, Any]:
    """Formata dados da API Speedio"""
    return {
        "CNPJ": data.get('CNPJ', ''),
        "Razão Social": data.get('RAZAO SOCIAL', ''),
        "Nome Fantasia": data.get('NOME FANTASIA', ''),
        "Situação Cadastral": data.get('STATUS', ''),
        "Data de Abertura": data.get('DATA ABERTURA', ''),
        "Porte": data.get('PORTE', ''),
        "Natureza Jurídica": data.get('NATUREZA JURIDICA', ''),
        "Telefone": data.get('TELEFONE', ''),
        "Email": data.get('EMAIL', ''),
        "Endereço": f"{data.get('LOGRADOURO', '')}, {data.get('NUMERO', '')} - "
                    f"{data.get('BAIRRO', '')}, {data.get('MUNICIPIO', '')} - "
                    f"{data.get('UF', '')}, {data.get('CEP', '')}",
        "Atividade Principal": [{
            'Código': data.get('CNAE PRINCIPAL CODIGO', ''),
            'Descrição': data.get('CNAE PRINCIPAL DESCRICAO', '')
        }],
        "Atividades Secundárias": [],
        "Sócios": [],
        "Simples Nacional": {
            "Optante": data.get('SIMPLES', '') == 'SIM',
            "Data de Opção": "",
            "Data de Exclusão": ""
        },
        "MEI": {
            "Optante": False,
            "Data de Opção": "",
            "Data de Exclusão": ""
        },
        "Capital Social": data.get('CAPITAL SOCIAL', ''),
        "Última Atualização": data.get('DATA SITUACAO', ''),
        "Status": "Sucesso (API Speedio)",
        "Detalhes API": f"Consulta realizada em {time.strftime('%Y-%m-%d %H:%M:%S')}"
    }

def buscar_dominio_email(email: str) -> dict:
    """Busca informações do domínio usando Clearbit (requer API key)"""
    if not email or '@' not in email:
        return {}
    dominio = email.split('@')[-1]
    try:
        response = requests.get(
            f"https://company.clearbit.com/v2/companies/find?domain={dominio}", 
            auth=(CLEARBIT_API_KEY, ''),
            timeout=5
        )
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.warning(f"Erro ao buscar informações de domínio: {str(e)}")
    return {}

def buscar_redes_sociais(nome: str) -> list:
    """Busca menções em redes sociais (requer API key)"""
    try:
        response = requests.get(
            f"https://api.social-searcher.com/v2/search?q={nome}&type=user&key={SOCIAL_SEARCHER_KEY}",
            timeout=10
        )
        if response.status_code == 200:
            return response.json().get('posts', [])[:5]  # Limita a 5 resultados
    except Exception as e:
        logger.warning(f"Erro ao buscar redes sociais: {str(e)}")
    return []

def verificar_pendencias(cnpj: str) -> dict:
    """Verifica pendências financeiras (implementação simulada)"""
    cnpj_limpo = limpar_cnpj(cnpj)
    try:
        # Simulação - em um sistema real, integraria com API de proteção de crédito
        pendencias = {
            'financeiras': random.choice([0, 1, 2, 3]),
            'fiscais': random.choice([0, 1]),
            'trabalhistas': random.choice([0, 1, 2]),
            'score_risco': random.randint(300, 900)
        }
        return {
            'status': 'success',
            'pendencias': pendencias
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }

def calcular_risco(resultado: dict) -> dict:
    """Calcula score de risco baseado nos dados da empresa"""
    if 'erro' in resultado:
        return {"score": -1, "nivel": "Erro na consulta"}
    score = 500  # Ponto de partida
    # Fatores positivos
    if resultado.get('Situação Cadastral') == 'ATIVA':
        score += 100
    if 'Simples Nacional' in resultado and resultado['Simples Nacional'].get('Optante'):
        score += 50
    # Empresas mais antigas são mais confiáveis
    if 'Data de Abertura' in resultado and resultado['Data de Abertura']:
        try:
            abertura = datetime.strptime(resultado['Data de Abertura'], '%Y-%m-%d')
            if abertura.year < 2010:
                score += 70
            elif abertura.year < 2015:
                score += 40
        except:
            pass
    # Fatores negativos
    if 'Pendências Financeiras' in resultado:
        pendencias = resultado['Pendências Financeiras'].get('pendencias', {})
        score -= pendencias.get('financeiras', 0) * 30
        score -= pendencias.get('fiscais', 0) * 50
        score -= pendencias.get('trabalhistas', 0) * 40
    # Normalizar score
    score = max(0, min(1000, score))
    # Classificação de risco
    if score >= 800:
        nivel = "Baixo Risco"
    elif score >= 600:
        nivel = "Risco Moderado"
    elif score >= 400:
        nivel = "Risco Médio"
    elif score >= 200:
        nivel = "Alto Risco"
    else:
        nivel = "Risco Muito Alto"
    return {"score": score, "nivel": nivel}

def consultar_alternativa(cnpj: str) -> Dict[str, Any]:
    """Consulta API alternativa caso a principal falhe"""
    cnpj_limpo = limpar_cnpj(cnpj)
    try:
        # API alternativa 1 - Speedio
        url = f"https://api-publica.speedio.com.br/buscarcnpj?cnpj={cnpj_limpo}"
        response = requests.get(url, timeout=10) # Timeout maior
        if response.status_code == 200:
            data = response.json()
            if not data.get('error'):
                logger.info(f"Consulta alternativa (Speedio) bem-sucedida para CNPJ: {cnpj}")
                return formatar_speedio(data)
    except Exception as e:
        logger.warning(f"Falha na consulta alternativa (Speedio) para CNPJ {cnpj}: {e}")
        pass

    # API alternativa 2 (fallback para a própria ReceitaWS - método direto)
    try:
        url = f"https://www.receitaws.com.br/v1/cnpj/{cnpj_limpo}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "OK":
                logger.info(f"Consulta alternativa (ReceitaWS direta) bem-sucedida para CNPJ: {cnpj}")
                # Reutiliza a função principal, mas sem retry para evitar loop
                return consultar_receitaws(cnpj, use_cache=False, skip_retry=True)
    except Exception as e:
        logger.warning(f"Falha na consulta alternativa (ReceitaWS direta) para CNPJ {cnpj}: {e}")
        pass

    logger.error(f"Todas as APIs alternativas falharam para CNPJ: {cnpj}")
    return {"erro": "Todas as APIs falharam"}

# --- Função principal de consulta ---
def consultar_receitaws(cnpj: str, use_cache=True, skip_retry=False) -> Dict[str, Any]:
    """
    Consulta dados de um CNPJ na API da ReceitaWS
    Args:
        cnpj: CNPJ a ser consultado
        use_cache: Usar cache se disponível
        skip_retry: Não tentar novamente em caso de erro
    Returns:
        Dicionário com os resultados da consulta
    """
    logger.info(f"Iniciando consulta para CNPJ: {cnpj}")
    # Validação inicial do CNPJ
    if not validar_cnpj(cnpj):
        logger.warning(f"CNPJ inválido fornecido: {cnpj}")
        return {"erro": "CNPJ inválido"}

    cnpj_limpo = limpar_cnpj(cnpj)

    # Verificar cache
    if use_cache:
        cached = buscar_cache(cnpj_limpo)
        if cached:
            logger.info(f"Usando dados em cache para CNPJ: {cnpj_limpo}")
            return cached

    # Controlar acesso à API
    api_monitor.wait_if_needed()
    api_monitor.record_request()

    url = f"https://receitaws.com.br/v1/cnpj/{cnpj_limpo}"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    try:
        response = requests.get(url, headers=headers, timeout=15) # Timeout aumentado
        if response.status_code == 200:
            data = response.json()
            if data.get("status") != "OK":
                logger.warning(f"Consulta inválida para CNPJ {cnpj_limpo}: {data.get('message')}")
                if not skip_retry:
                    # Tentar API alternativa
                    logger.info("Tentando API alternativa...")
                    return consultar_alternativa(cnpj)
                return {"erro": f"Consulta inválida: {data.get('message', 'sem mensagem')}"}

            # Processamento dos dados
            resultado = {
                "CNPJ": data.get("cnpj", ""),
                "Razão Social": data.get("nome", ""),
                "Nome Fantasia": data.get("fantasia", ""),
                "Situação Cadastral": data.get("situacao", ""),
                "Data de Abertura": data.get("abertura", ""),
                "Porte": data.get("porte", ""),
                "Natureza Jurídica": data.get("natureza_juridica", ""),
                "Telefone": data.get("telefone", ""),
                "Email": data.get("email", ""),
                "Endereço": formatar_endereco(data),
                "Atividade Principal": formatar_atividades(data.get("atividade_principal", [])),
                "Atividades Secundárias": formatar_atividades(data.get("atividades_secundarias", [])),
                "Sócios": formatar_socios(data.get("qsa", [])),
                "Simples Nacional": {
                    "Optante": data.get("simples", {}).get("optante", False),
                    "Data de Opção": data.get("simples", {}).get("data_opcao", ""),
                    "Data de Exclusão": data.get("simples", {}).get("data_exclusao", "")
                },
                "MEI": {
                    "Optante": data.get("simei", {}).get("optante", False),
                    "Data de Opção": data.get("simei", {}).get("data_opcao", ""),
                    "Data de Exclusão": data.get("simei", {}).get("data_exclusao", "")
                },
                "Capital Social": data.get("capital_social", ""),
                "Última Atualização": data.get("ultima_atualizacao", ""),
                "Status": "Sucesso",
                "Detalhes API": f"Consulta realizada em {time.strftime('%Y-%m-%d %H:%M:%S')}",
                "cache_date": datetime.now().isoformat()
            }

            # Informações adicionais (opcional, pode ser desativado para batch por performance)
            # resultado["Informações Complementares"] = {
            #     "Domínio": buscar_dominio_email(data.get("email", "")),
            #     "Redes Sociais": buscar_redes_sociais(data.get("nome", ""))
            # }

            # Verificação de pendências (simulada)
            resultado["Pendências Financeiras"] = verificar_pendencias(cnpj)

            # Análise de risco
            resultado["Análise de Risco"] = calcular_risco(resultado)

            # Salvar no cache
            salvar_consulta_cache(cnpj_limpo, resultado)
            return resultado

        elif response.status_code == 429:
            logger.warning("Limite de requisições excedido na ReceitaWS")
            if not skip_retry:
                time.sleep(5)
                return consultar_alternativa(cnpj)
            return {"erro": "Limite de requisições excedido. Tente novamente mais tarde."}

        elif response.status_code == 504:
            logger.warning("Timeout na consulta da ReceitaWS")
            if not skip_retry:
                return consultar_alternativa(cnpj)
            return {"erro": "Timeout na consulta. CNPJ pode estar fora do cache da API."}

        else:
            logger.error(f"Erro inesperado na API: {response.status_code}")
            if not skip_retry:
                return consultar_alternativa(cnpj)
            return {"erro": f"Erro inesperado: {response.status_code}"}

    except requests.exceptions.Timeout:
        logger.error("Timeout na conexão com a API")
        if not skip_retry:
            return consultar_alternativa(cnpj)
        return {"erro": "Timeout na conexão com a API"}

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de conexão: {str(e)}")
        if not skip_retry:
            return consultar_alternativa(cnpj)
        return {"erro": f"Erro de conexão: {str(e)}"}

# --- Funções de Formatação e Exportação ---
def formatar_resultado(resultado: Dict[str, Any]) -> str:
    """Formata o resultado para exibição amigável"""
    if "erro" in resultado:
        return f"Erro: {resultado['erro']}"

    output = []
    output.append("=" * 60)
    output.append(f"CONSULTA CNPJ: {resultado.get('CNPJ', '')}")
    output.append("=" * 60)

    # Informações básicas
    output.append("\n[INFORMAÇÕES BÁSICAS]")
    output.append(f"Razão Social: {resultado.get('Razão Social', '')}")
    output.append(f"Nome Fantasia: {resultado.get('Nome Fantasia', '')}")
    output.append(f"Situação Cadastral: {resultado.get('Situação Cadastral', '')}")
    output.append(f"Data de Abertura: {resultado.get('Data de Abertura', '')}")
    output.append(f"Porte: {resultado.get('Porte', '')}")
    output.append(f"Natureza Jurídica: {resultado.get('Natureza Jurídica', '')}")

    # Contato
    output.append("\n[CONTATO]")
    output.append(f"Telefone: {resultado.get('Telefone', '')}")
    output.append(f"Email: {resultado.get('Email', '')}")
    output.append(f"Endereço: {resultado.get('Endereço', '')}")

    # Atividades
    output.append("\n[ATIVIDADES]")
    if resultado.get('Atividade Principal'):
        for atv in resultado['Atividade Principal']:
            output.append(f"Principal: {atv.get('Código', '')} - {atv.get('Descrição', '')}")
    if resultado.get('Atividades Secundárias'):
        output.append("\nSecundárias:")
        for i, atv in enumerate(resultado['Atividades Secundárias'], 1):
            output.append(f"  {i}. {atv.get('Código', '')} - {atv.get('Descrição', '')}")

    # Sócios
    if resultado.get('Sócios'):
        output.append("\n[SÓCIOS]")
        for i, socio in enumerate(resultado['Sócios'], 1):
            output.append(f"  {i}. {socio.get('Nome', '')} ({socio.get('Qualificação', '')})")
            if socio.get('Documento'):
                output.append(f"      Documento: {socio.get('Documento', '')}")

    # Regimes especiais
    output.append("\n[REGIMES ESPECIAIS]")
    sn = resultado.get('Simples Nacional', {})
    output.append(f"Simples Nacional: {'Sim' if sn.get('Optante') else 'Não'}")
    if sn.get('Data de Opção'):
        output.append(f"  Data de Opção: {sn.get('Data de Opção')}")
    if sn.get('Data de Exclusão'):
        output.append(f"  Data de Exclusão: {sn.get('Data de Exclusão')}")

    mei = resultado.get('MEI', {})
    output.append(f"MEI: {'Sim' if mei.get('Optante') else 'Não'}")
    if mei.get('Data de Opção'):
        output.append(f"  Data de Opção: {mei.get('Data de Opção')}")
    if mei.get('Data de Exclusão'):
        output.append(f"  Data de Exclusão: {mei.get('Data de Exclusão')}")

    # Pendências financeiras
    if resultado.get('Pendências Financeiras', {}).get('status') == 'success':
        pendencias = resultado['Pendências Financeiras']['pendencias']
        output.append("\n[PENDÊNCIAS FINANCEIRAS]")
        output.append(f"Financeiras: {pendencias.get('financeiras', 0)}")
        output.append(f"Fiscais: {pendencias.get('fiscais', 0)}")
        output.append(f"Trabalhistas: {pendencias.get('trabalhistas', 0)}")
        output.append(f"Score de Risco: {pendencias.get('score_risco', 0)}")

    # Análise de risco
    if resultado.get('Análise de Risco'):
        risco = resultado['Análise de Risco']
        output.append("\n[ANÁLISE DE RISCO]")
        output.append(f"Score: {risco.get('score', 0)}")
        output.append(f"Nível: {risco.get('nivel', '')}")

    # Informações complementares (desativadas para performance no batch)
    # if resultado.get('Informações Complementares', {}).get('Domínio'):
    #     dominio = resultado['Informações Complementares']['Domínio']
    #     output.append("\n[INFORMAÇÕES DO DOMÍNIO]")
    #     output.append(f"Nome: {dominio.get('name', '')}")
    #     output.append(f"Domínio: {dominio.get('domain', '')}")
    #     output.append(f"Site: {dominio.get('url', '')}")
    #     output.append(f"Setor: {dominio.get('category', {}).get('sector', '')}")

    # Redes sociais (desativadas para performance no batch)
    # if resultado.get('Informações Complementares', {}).get('Redes Sociais'):
    #     redes = resultado['Informações Complementares']['Redes Sociais']
    #     output.append("\n[MENÇÕES EM REDES SOCIAIS]")
    #     for i, rede in enumerate(redes[:3], 1):  # Limita a 3 resultados
    #         output.append(f"  {i}. {rede.get('network', '')}: {rede.get('text', '')[:60]}...")

    # Outras informações
    output.append("\n[OUTRAS INFORMAÇÕES]")
    output.append(f"Capital Social: {resultado.get('Capital Social', '')}")
    output.append(f"Última Atualização: {resultado.get('Última Atualização', '')}")
    output.append(f"\n{resultado.get('Detalhes API', '')}")

    return '\n'.join(output)

def exportar_resultado(resultado: dict, formato: str = 'json'):
    """Exporta o resultado para diferentes formatos"""
    if 'erro' in resultado:
        return "Não é possível exportar resultado com erro"

    cnpj_limpo = limpar_cnpj(resultado['CNPJ'])
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"cnpj_{cnpj_limpo}_{timestamp}"

    try:
        if formato == 'json':
            with open(f"{filename}.json", 'w', encoding='utf-8') as f:
                json.dump(resultado, f, ensure_ascii=False, indent=4)
            return f"Resultado exportado como {filename}.json"

        elif formato == 'csv':
            # Achatar dados para CSV
            flat_data = {
                'CNPJ': resultado.get('CNPJ', ''),
                'Razao_Social': resultado.get('Razão Social', ''),
                'Nome_Fantasia': resultado.get('Nome Fantasia', ''),
                'Situacao_Cadastral': resultado.get('Situação Cadastral', ''),
                'Data_Abertura': resultado.get('Data de Abertura', ''),
                'Porte': resultado.get('Porte', ''),
                'Natureza_Juridica': resultado.get('Natureza Jurídica', ''),
                'Telefone': resultado.get('Telefone', ''),
                'Email': resultado.get('Email', ''),
                'Endereco': resultado.get('Endereço', ''),
                'Capital_Social': resultado.get('Capital Social', ''),
                'Score_Risco': resultado.get('Análise de Risco', {}).get('score', ''),
                'Nivel_Risco': resultado.get('Análise de Risco', {}).get('nivel', '')
            }
            with open(f"{filename}.csv", 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=flat_data.keys())
                writer.writeheader()
                writer.writerow(flat_data)
            return f"Resultado exportado como {filename}.csv"

        elif formato == 'txt':
            with open(f"{filename}.txt", 'w', encoding='utf-8') as f:
                f.write(formatar_resultado(resultado))
            return f"Resultado exportado como {filename}.txt"

        elif formato == 'html':
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Consulta CNPJ - {resultado.get('Razão Social', '')}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    h1, h2 {{ color: #333; }}
                    table {{ border-collapse: collapse; width: 100%; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                    .risco-baixo {{ color: green; }}
                    .risco-moderado {{ color: orange; }}
                    .risco-medio {{ color: #cc9900; }}
                    .risco-alto {{ color: red; }}
                </style>
            </head>
            <body>
                <h1>Consulta CNPJ: {resultado.get('CNPJ', '')}</h1>
                <h2>{resultado.get('Razão Social', '')}</h2>
                <h3>Informações Básicas</h3>
                <table>
                    <tr><th>Campo</th><th>Valor</th></tr>
                    <tr><td>Nome Fantasia</td><td>{resultado.get('Nome Fantasia', '')}</td></tr>
                    <tr><td>Situação Cadastral</td><td>{resultado.get('Situação Cadastral', '')}</td></tr>
                    <tr><td>Data de Abertura</td><td>{resultado.get('Data de Abertura', '')}</td></tr>
                    <tr><td>Porte</td><td>{resultado.get('Porte', '')}</td></tr>
                    <tr><td>Natureza Jurídica</td><td>{resultado.get('Natureza Jurídica', '')}</td></tr>
                </table>
                <h3>Contato</h3>
                <table>
                    <tr><th>Campo</th><th>Valor</th></tr>
                    <tr><td>Telefone</td><td>{resultado.get('Telefone', '')}</td></tr>
                    <tr><td>Email</td><td>{resultado.get('Email', '')}</td></tr>
                    <tr><td>Endereço</td><td>{resultado.get('Endereço', '')}</td></tr>
                </table>
                <h3>Análise de Risco</h3>
                <table>
                    <tr><th>Score</th><th>Nível</th></tr>
                    <tr>
                        <td>{resultado.get('Análise de Risco', {}).get('score', '')}</td>
                        <td class="risco-{resultado.get('Análise de Risco', {}).get('nivel', '').lower().replace(' ', '-')}">
                            {resultado.get('Análise de Risco', {}).get('nivel', '')}
                        </td>
                    </tr>
                </table>
                <p>Relatório gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </body>
            </html>
            """
            with open(f"{filename}.html", 'w', encoding='utf-8') as f:
                f.write(html_content)
            return f"Resultado exportado como {filename}.html"

        else:
            return "Formato de exportação inválido"

    except Exception as e:
        return f"Erro ao exportar resultado: {str(e)}"

# --- Funções para Batch Processing ---
def carregar_lista_cnpjs(filepath: str) -> List[str]:
    """Carrega uma lista de CNPJs de um arquivo txt ou csv."""
    cnpjs = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            # Tenta ler como CSV primeiro (separado por vírgula ou ponto e vírgula)
            content = f.read()
            if ',' in content or ';' in content:
                delimiter = ',' if ',' in content else ';'
                reader = csv.reader(content.splitlines(), delimiter=delimiter)
                for row in reader:
                    if row: # Ignora linhas vazias
                        cnpjs.append(row[0].strip()) # Assume que o CNPJ está na primeira coluna
            else:
                # Se não tiver delimitador, assume um CNPJ por linha
                lines = content.splitlines()
                cnpjs = [line.strip() for line in lines if line.strip()]

        logger.info(f"{len(cnpjs)} CNPJs carregados de {filepath}")
        return cnpjs
    except Exception as e:
        logger.error(f"Erro ao carregar lista de CNPJs: {e}")
        raise e

def processar_lote(cnpjs: List[str], progress_callback=None, final_callback=None):
    """Processa uma lista de CNPJs em lote."""
    resultados = []
    total = len(cnpjs)
    logger.info(f"Iniciando processamento em lote de {total} CNPJs")

    for i, cnpj in enumerate(cnpjs):
        if progress_callback:
            progress_callback(i + 1, total, cnpj)

        logger.debug(f"Consultando CNPJ {i+1}/{total}: {cnpj}")
        resultado = consultar_receitaws(cnpj)
        resultados.append(resultado)

        # Pequeno delay entre requisições para ser gentil com a API
        if i < total - 1: # Não dormir após o último
            time.sleep(BATCH_DELAY_BETWEEN_REQUESTS)

    logger.info("Processamento em lote concluído")
    if final_callback:
        final_callback(resultados)

# --- Funções para Extração de Dados ---
def extrair_dados(resultados: List[Dict], campos: List[str]) -> List[Dict[str, Any]]:
    """Extrai campos específicos dos resultados."""
    dados_extraidos = []
    for res in resultados:
        if 'erro' in res:
            # Adiciona uma entrada com erro para manter a ordem
             dados_extraidos.append({'CNPJ': res.get('CNPJ', 'Desconhecido'), 'Erro': res['erro']})
             continue

        item = {}
        for campo in campos:
            # Tratamento especial para campos aninhados ou listas
            if campo == 'Atividade Principal':
                ativ_principal = res.get('Atividade Principal', [{}])
                if ativ_principal:
                    item[campo] = f"{ativ_principal[0].get('Código', '')} - {ativ_principal[0].get('Descrição', '')}"
                else:
                    item[campo] = ''
            elif campo == 'Sócios':
                 # Concatena nomes dos sócios
                 socios = res.get('Sócios', [])
                 nomes_socios = [s.get('Nome', '') for s in socios if s.get('Nome')]
                 item[campo] = '; '.join(nomes_socios) if nomes_socios else ''
            elif campo == 'Análise de Risco':
                 risco = res.get('Análise de Risco', {})
                 item['Score de Risco'] = risco.get('score', '')
                 item['Nível de Risco'] = risco.get('nivel', '')
            elif campo in res:
                item[campo] = res[campo]
            else:
                # Se o campo não existir diretamente, tenta encontrar em sub-dicionários
                # Esta lógica pode ser expandida conforme necessário
                item[campo] = '' # Ou um valor padrão

        dados_extraidos.append(item)
    return dados_extraidos


def salvar_dados_extraidos(dados: List[Dict[str, Any]], filepath: str):
    """Salva os dados extraídos em um arquivo CSV."""
    if not dados:
        logger.warning("Nenhum dado para salvar.")
        return

    try:
        # Determinar os cabeçalhos dinamicamente
        fieldnames = set()
        for item in dados:
            fieldnames.update(item.keys())
        fieldnames = sorted(list(fieldnames))

        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for item in dados:
                writer.writerow(item)

        logger.info(f"Dados extraídos salvos em {filepath}")
    except Exception as e:
        logger.error(f"Erro ao salvar dados extraídos: {e}")
        raise e


# --- Interface Gráfica ---
class CNPJApp:
    def __init__(self, master):
        self.master = master
        master.title("Consultor de CNPJ Avançado")
        master.geometry("1000x700") # Janela maior

        # Configurar estilo
        self.style = ttk.Style()
        self.style.configure('TLabel', font=('Arial', 10))
        self.style.configure('TButton', font=('Arial', 10))
        self.style.configure('TEntry', font=('Arial', 10))

        # Notebook para abas
        self.notebook = ttk.Notebook(master)
        self.notebook.pack(pady=10, padx=10, fill='both', expand=True)

        # --- Aba de Consulta Individual ---
        self.tab_individual = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_individual, text='Consulta Individual')

        # Frame de entrada Individual
        input_frame_ind = ttk.Frame(self.tab_individual)
        input_frame_ind.pack(pady=10, fill='x')

        ttk.Label(input_frame_ind, text="CNPJ:").pack(side='left', padx=5)
        self.cnpj_entry = ttk.Entry(input_frame_ind, width=30)
        self.cnpj_entry.pack(side='left', padx=5)
        self.cnpj_entry.focus()
        self.consultar_btn = ttk.Button(input_frame_ind, text="Consultar", command=self.consultar_individual)
        self.consultar_btn.pack(side='left', padx=5)

        # Frame de exportação Individual
        export_frame_ind = ttk.Frame(self.tab_individual)
        export_frame_ind.pack(pady=5, fill='x')

        ttk.Label(export_frame_ind, text="Exportar como:").pack(side='left', padx=5)
        self.export_format = tk.StringVar(value='txt')
        formats = [('Texto', 'txt'), ('JSON', 'json'), ('CSV', 'csv'), ('HTML', 'html')]
        for text, value in formats:
            rb = ttk.Radiobutton(export_frame_ind, text=text, variable=self.export_format, value=value)
            rb.pack(side='left', padx=5)

        self.export_btn = ttk.Button(export_frame_ind, text="Exportar", command=self.exportar_individual, state='disabled')
        self.export_btn.pack(side='left', padx=5)

        # Área de resultado Individual
        self.result_text = scrolledtext.ScrolledText(
            self.tab_individual,
            wrap=tk.WORD,
            font=('Courier New', 10)
        )
        self.result_text.pack(pady=10, padx=10, fill='both', expand=True)
        self.result_text.insert(tk.END, "Digite um CNPJ e clique em Consultar...")
        self.result_text.config(state='disabled')

        # --- Aba de Consulta em Lote ---
        self.tab_batch = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_batch, text='Consulta em Lote')

        # Frame para carregar arquivo
        load_frame = ttk.Frame(self.tab_batch)
        load_frame.pack(pady=10, fill='x')

        self.lista_filepath = tk.StringVar()
        ttk.Label(load_frame, text="Arquivo de CNPJs:").pack(side='left', padx=5)
        self.lista_entry = ttk.Entry(load_frame, textvariable=self.lista_filepath, width=50, state='readonly')
        self.lista_entry.pack(side='left', padx=5)
        self.load_btn = ttk.Button(load_frame, text="Carregar Lista", command=self.carregar_lista)
        self.load_btn.pack(side='left', padx=5)

        # Frame para iniciar processo
        process_frame = ttk.Frame(self.tab_batch)
        process_frame.pack(pady=5, fill='x')

        self.process_btn = ttk.Button(process_frame, text="Processar Lote", command=self.iniciar_processamento_lote, state='disabled')
        self.process_btn.pack(side='left', padx=5)

        # Barra de progresso
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(process_frame, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(side='left', padx=10, fill='x', expand=True)

        # Área de log do processo
        self.batch_log_text = scrolledtext.ScrolledText(
            self.tab_batch,
            wrap=tk.WORD,
            font=('Courier New', 9),
            height=10
        )
        self.batch_log_text.pack(pady=10, padx=10, fill='both', expand=True)
        self.batch_log_text.insert(tk.END, "Carregue uma lista de CNPJs para começar o processamento em lote...\n")
        self.batch_log_text.config(state='disabled')

        # Frame para exportação do lote
        batch_export_frame = ttk.Frame(self.tab_batch)
        batch_export_frame.pack(pady=5, fill='x')

        self.export_batch_btn = ttk.Button(batch_export_frame, text="Exportar Resultados Completos", command=self.exportar_resultados_lote, state='disabled')
        self.export_batch_btn.pack(side='left', padx=5)

        # --- Aba de Extração de Dados ---
        self.tab_extract = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_extract, text='Extrair Dados')

        # Frame para carregar arquivo de resultados
        load_results_frame = ttk.Frame(self.tab_extract)
        load_results_frame.pack(pady=10, fill='x')

        self.resultados_filepath = tk.StringVar()
        ttk.Label(load_results_frame, text="Arquivo de Resultados:").pack(side='left', padx=5)
        self.resultados_entry = ttk.Entry(load_results_frame, textvariable=self.resultados_filepath, width=50, state='readonly')
        self.resultados_entry.pack(side='left', padx=5)
        self.load_results_btn = ttk.Button(load_results_frame, text="Carregar Resultados", command=self.carregar_resultados)
        self.load_results_btn.pack(side='left', padx=5)

        # Frame para seleção de campos
        fields_frame = ttk.Frame(self.tab_extract)
        fields_frame.pack(pady=5, fill='x')

        ttk.Label(fields_frame, text="Campos para Extrair:").pack(anchor='w', padx=5)
        self.campos_vars = {}
        campos_disponiveis = [
            "CNPJ", "Razão Social", "Nome Fantasia", "Situação Cadastral",
            "Telefone", "Email", "Endereço", "Atividade Principal",
            "Sócios", "Análise de Risco", "Porte", "Natureza Jurídica"
        ]
        # Organiza os campos em duas colunas
        col1_frame = ttk.Frame(fields_frame)
        col1_frame.pack(side='left', padx=20)
        col2_frame = ttk.Frame(fields_frame)
        col2_frame.pack(side='left', padx=20)

        for i, campo in enumerate(campos_disponiveis):
            var = tk.BooleanVar()
            chk = ttk.Checkbutton((col1_frame if i % 2 == 0 else col2_frame), text=campo, variable=var)
            chk.pack(anchor='w')
            self.campos_vars[campo] = var

        # Botão para extrair
        extract_btn_frame = ttk.Frame(self.tab_extract)
        extract_btn_frame.pack(pady=10)

        self.extract_btn = ttk.Button(extract_btn_frame, text="Extrair Dados", command=self.extrair_e_salvar, state='disabled')
        self.extract_btn.pack()

        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Pronto")
        status_bar = ttk.Label(master, textvariable=self.status_var, relief='sunken', anchor='w')
        status_bar.pack(side='bottom', fill='x')

        # Armazenar último resultado individual e resultados do lote
        self.ultimo_resultado = None
        self.resultados_lote = []
        self.resultados_carregados = [] # Para extração

        # Configurar cache
        setup_cache_db()

    # --- Métodos para Consulta Individual ---
    def consultar_individual(self):
        cnpj = self.cnpj_entry.get().strip()
        if not cnpj:
            messagebox.showwarning("Atenção", "Por favor, digite um CNPJ.")
            return
        self.status_var.set("Consultando...")
        self.consultar_btn.config(state='disabled')
        self.master.update()
        try:
            resultado = consultar_receitaws(cnpj)
            self.ultimo_resultado = resultado
            formatted = formatar_resultado(resultado)
            self.result_text.config(state='normal')
            self.result_text.delete(1.0, tk.END)
            self.result_text.insert(tk.END, formatted)
            self.result_text.config(state='disabled')
            self.export_btn.config(state='normal')
            self.status_var.set("Consulta concluída")
        except Exception as e:
            messagebox.showerror("Erro", f"Ocorreu um erro na consulta: {str(e)}")
            self.status_var.set("Erro na consulta")
        finally:
            self.consultar_btn.config(state='normal')

    def exportar_individual(self):
        if not self.ultimo_resultado:
            messagebox.showwarning("Atenção", "Nenhum resultado disponível para exportação.")
            return
        formato = self.export_format.get()
        resultado = exportar_resultado(self.ultimo_resultado, formato)
        if resultado.startswith("Erro"):
            messagebox.showerror("Erro", resultado)
        else:
            messagebox.showinfo("Sucesso", resultado)
            self.status_var.set(resultado)

    # --- Métodos para Consulta em Lote ---
    def carregar_lista(self):
        filepath = filedialog.askopenfilename(
            title="Selecione o arquivo de CNPJs",
            filetypes=(("Arquivos de Texto", "*.txt"), ("Arquivos CSV", "*.csv"), ("Todos os Arquivos", "*.*"))
        )
        if filepath:
            try:
                self.lista_filepath.set(filepath)
                self.cnpjs_para_processar = carregar_lista_cnpjs(filepath)
                self.process_btn.config(state='normal')
                self.adicionar_log_lote(f"Lista carregada com {len(self.cnpjs_para_processar)} CNPJs.\n")
                self.status_var.set(f"Lista carregada: {os.path.basename(filepath)}")
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao carregar a lista: {e}")
                self.status_var.set("Erro ao carregar lista")

    def adicionar_log_lote(self, message):
        self.batch_log_text.config(state='normal')
        self.batch_log_text.insert(tk.END, message)
        self.batch_log_text.see(tk.END) # Auto-scroll
        self.batch_log_text.config(state='disabled')

    def atualizar_progresso(self, current, total, cnpj_atual):
        porcentagem = (current / total) * 100
        self.progress_var.set(porcentagem)
        self.adicionar_log_lote(f"Processando {current}/{total}: {cnpj_atual}\n")
        self.status_var.set(f"Processando {current}/{total}")
        self.master.update_idletasks() # Atualiza a GUI

    def finalizar_processamento_lote(self, resultados):
        self.resultados_lote = resultados
        self.export_batch_btn.config(state='normal')
        self.adicionar_log_lote(f"\nProcessamento concluído! {len(resultados)} resultados obtidos.\n")
        self.status_var.set("Processamento em lote concluído")
        self.process_btn.config(state='normal') # Reabilita o botão
        messagebox.showinfo("Concluído", "Processamento em lote finalizado!")

    def iniciar_processamento_lote(self):
        if not hasattr(self, 'cnpjs_para_processar') or not self.cnpjs_para_processar:
             messagebox.showwarning("Atenção", "Nenhuma lista de CNPJs foi carregada.")
             return

        self.process_btn.config(state='disabled')
        self.export_batch_btn.config(state='disabled')
        self.progress_var.set(0)
        self.batch_log_text.config(state='normal')
        self.batch_log_text.delete(1.0, tk.END)
        self.batch_log_text.config(state='disabled')
        self.resultados_lote = []

        # Executa o processamento em uma thread separada para não travar a GUI
        thread = threading.Thread(
            target=processar_lote,
            args=(self.cnpjs_para_processar,),
            kwargs={
                'progress_callback': self.atualizar_progresso,
                'final_callback': self.finalizar_processamento_lote
            }
        )
        thread.daemon = True
        thread.start()

    def exportar_resultados_lote(self):
        if not self.resultados_lote:
            messagebox.showwarning("Atenção", "Nenhum resultado de lote disponível para exportar.")
            return

        # Pergunta ao usuário onde salvar o arquivo
        filepath = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("Arquivo JSON", "*.json"), ("Todos os Arquivos", "*.*")],
            title="Salvar Resultados do Lote"
        )
        if filepath:
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(self.resultados_lote, f, ensure_ascii=False, indent=4)
                messagebox.showinfo("Sucesso", f"Resultados do lote salvos em:\n{filepath}")
                self.status_var.set(f"Lote exportado: {os.path.basename(filepath)}")
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao salvar o arquivo: {e}")
                self.status_var.set("Erro ao exportar lote")

    # --- Métodos para Extração de Dados ---
    def carregar_resultados(self):
        filepath = filedialog.askopenfilename(
            title="Selecione o arquivo de Resultados (JSON)",
            filetypes=(("Arquivos JSON", "*.json"), ("Todos os Arquivos", "*.*"))
        )
        if filepath:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    self.resultados_carregados = json.load(f)
                self.resultados_filepath.set(filepath)
                self.extract_btn.config(state='normal')
                self.adicionar_log_lote(f"Resultados carregados de {os.path.basename(filepath)} ({len(self.resultados_carregados)} itens).\n")
                self.status_var.set(f"Resultados carregados: {os.path.basename(filepath)}")
            except Exception as e:
                messagebox.showerror("Erro", f"Falha ao carregar os resultados: {e}")
                self.status_var.set("Erro ao carregar resultados")

    def extrair_e_salvar(self):
        if not self.resultados_carregados:
            messagebox.showwarning("Atenção", "Nenhum resultado foi carregado para extração.")
            return

        # Obter campos selecionados
        campos_selecionados = [campo for campo, var in self.campos_vars.items() if var.get()]
        if not campos_selecionados:
            messagebox.showwarning("Atenção", "Por favor, selecione pelo menos um campo para extrair.")
            return

        try:
            dados_extraidos = extrair_dados(self.resultados_carregados, campos_selecionados)

            # Pergunta ao usuário onde salvar o arquivo CSV extraído
            filepath = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("Arquivo CSV", "*.csv"), ("Todos os Arquivos", "*.*")],
                title="Salvar Dados Extraídos"
            )
            if filepath:
                salvar_dados_extraidos(dados_extraidos, filepath)
                messagebox.showinfo("Sucesso", f"Dados extraídos salvos em:\n{filepath}")
                self.status_var.set(f"Dados extraídos: {os.path.basename(filepath)}")
        except Exception as e:
            messagebox.showerror("Erro", f"Falha na extração: {e}")
            self.status_var.set("Erro na extração")

# --- Função principal ---
def main():
    # Inicializar sistema de cache
    setup_cache_db()
    # Iniciar interface gráfica
    root = tk.Tk()
    app = CNPJApp(root)
    root.mainloop()

if __name__ == "__main__":
    logger.info("Iniciando sistema de consulta de CNPJ Avançado")
    main()