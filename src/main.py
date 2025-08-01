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
import threading

# --- Configurações globais ---
TOKEN = os.getenv('TOKEN')  # Sugestão: use variável de ambiente
CLEARBIT_API_KEY = os.getenv('CLEARBIT_API_KEY')  # Sugestão: use variável de ambiente
SOCIAL_SEARCHER_KEY = os.getenv('SOCIAL_SEARCHER_KEY')  # Sugestão: use variável de ambiente
CACHE_DAYS = 30
MAX_API_REQUESTS = 3
API_REQUEST_WINDOW = 60  # segundos
BATCH_DELAY_BETWEEN_REQUESTS = 1 # Segundos entre requisições em lote para ser gentil com a API

# --- Configuração de logging ---
def setup_logger():
    logger = logging.getLogger('cnpj_consultor')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler = RotatingFileHandler('cnpj_consultor.log', maxBytes=10*1024*1024, backupCount=5)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
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

api_monitor = APIMonitor()

# --- Configuração de cache ---
def setup_cache_db():
    conn = sqlite3.connect('cnpj_cache.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS consultas (cnpj TEXT PRIMARY KEY, data TEXT, resultado TEXT)''')
    conn.commit()
    conn.close()

# ... [o restante do código original permanece, mas com melhorias mencionadas] ... 
    <agregue as operações de cache, consulta, etc. utilizando as melhorias sugeridas, como tratamento de exceções e modularização> ...

