# Consultor de CNPJ

Um aplicativo desktop Python para consultar dados de empresas brasileiras através do CNPJ, utilizando a API da ReceitaWS e outras fontes alternativas. Permite consultas individuais, em lote e extração de dados específicos.

## Funcionalidades

*   **Consulta Individual:** Busca dados detalhados de um CNPJ via interface gráfica.
*   **Consulta em Lote:** Processa uma lista de CNPJs a partir de um arquivo (txt/csv) e armazena os resultados.
*   **Cache Inteligente:** Evita consultas repetidas usando um banco de dados SQLite local.
*   **Controle de API:** Respeita limites de requisição da API para evitar erros.
*   **Exportação de Dados:** Exporta resultados individuais ou completos em formatos JSON, CSV, TXT, HTML.
*   **Extração de Dados:** Carrega resultados de lotes e extrai campos específicos (ex: Nome, Email, Telefone) para um novo CSV.
*   **Análise de Risco:** Calcula um score de risco preliminar com base nos dados da empresa.
*   **APIs Alternativas:** Tenta APIs alternativas caso a principal falhe.

## Tecnologias Utilizadas

*   Python 3.x
*   `tkinter` (Interface Gráfica)
*   `requests` (Requisições HTTP)
*   `sqlite3` (Cache local)
*   `logging` (Registro de logs)

## Como Instalar e Executar

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/SEU_USUARIO_GITHUB/consultor-cnpj.git
    cd consultor-cnpj
    ```
2.  **(Recomendado) Crie um Ambiente Virtual:**
    ```bash
    python -m venv venv
    # No Windows:
    venv\Scripts\activate
    # No Linux/Mac:
    # source venv/bin/activate
    ```
3.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Execute o aplicativo:**
    ```bash
    python main.py
    ```

## Como Usar

1.  **Consulta Individual:**
    *   Digite o CNPJ na aba correspondente e clique em "Consultar".
    *   Visualize os dados na tela.
    *   Escolha o formato e clique em "Exportar".
2.  **Consulta em Lote:**
    *   Na aba "Consulta em Lote", clique em "Carregar Lista" e selecione um arquivo `.txt` (um CNPJ por linha) ou `.csv`.
    *   Clique em "Processar Lote". Acompanhe o progresso.
    *   Após concluir, clique em "Exportar Resultados Completos" para salvar o JSON com todos os dados.
3.  **Extração de Dados:**
    *   Na aba "Extrair Dados", clique em "Carregar Resultados" e selecione o arquivo JSON gerado no passo anterior.
    *   Marque os campos que deseja extrair (ex: Razão Social, Email, Telefone).
    *   Clique em "Extrair Dados" e escolha onde salvar o novo arquivo CSV com os dados consolidados.

## Configuração (Opcional)

*   **API Keys:** Para funcionalidades extras (Clearbit, Social Searcher), substitua as chaves no início do `main.py`.
*   **Configurações de Cache/API:** Ajuste `CACHE_DAYS`, `MAX_API_REQUESTS`, `API_REQUEST_WINDOW` no `main.py`.

## Contribuições

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou pull requests.

## Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.