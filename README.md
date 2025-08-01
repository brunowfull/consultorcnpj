# Consultor de CNPJ Avançado

Este projeto é um sistema de consulta de CNPJ, que utiliza APIs para buscar informações sobre empresas brasileiras. O software fornece uma interface gráfica e permite a consulta em lote de múltiplos CNPJs.

## Requisitos
- Python 3.x
- Bibliotecas:
  - requests
  - sqlite3
  - logging
  - tkinter

## Instalação
1. Clone o repositório:
    ```bash
    git clone https://github.com/brunowfull/consultorcnpj.git
    cd consultorcnpj
    ```
2. Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```
3. Configure suas chaves de API no ambiente:
    ```bash
    export TOKEN=<seu_token>
    export CLEARBIT_API_KEY=<sua_clearbit_api_key>
    export SOCIAL_SEARCHER_KEY=<sua_social_searcher_key>
    ```

## Uso
Execute o script:
```bash
python src/main.py
```

## Contribuições
Sinta-se à vontade para abrir issues e pull requests!