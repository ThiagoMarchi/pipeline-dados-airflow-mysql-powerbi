# Projeto de Pipeline de Dados com Airflow, Docker e MySQL

## 📄 Resumo
Este projeto demonstra a construção de um pipeline de dados completo (ELT - Extract, Load, Transform) utilizando ferramentas modernas de engenharia de dados. O pipeline extrai dados de veículos de uma API pública, os processa, armazena em um banco de dados MySQL e, finalmente, gera um conjunto de dados modelado e pronto para análise em ferramentas de Business Intelligence como o Power BI.

O projeto foi totalmente containerizado com Docker e orquestrado com Apache Airflow.

---

## 🏗️ Arquitetura do Pipeline

O fluxo de dados segue a seguinte arquitetura:

```
[Fontes de Dados] ---> [Extração & Carga (ELT)] ---> [Armazenamento] ---> [Visualização]
      |                           |                        |                    |
[APIs Públicas] ---> [Airflow (Python/Pandas)] ---> [MySQL (Docker)] ---> [Power BI]
```

---

## 🛠️ Tecnologias Utilizadas

* **Orquestração:** Apache Airflow
* **Containerização:** Docker & Docker Compose
* **Banco de Dados:** MySQL 8.0
* **Linguagem Principal:** Python 3.12
* **Bibliotecas Python:** Pandas, SQLAlchemy, Requests, Faker
* **Ferramenta de BI:** Microsoft Power BI
* **Ambiente de Desenvolvimento:** WSL 2 (Ubuntu)

---

## 📁 Estrutura do Projeto

```
/meu_novo_pipeline
|
├── dags/                  # Contém os arquivos .py das DAGs do Airflow
│   ├── dag_build_dim_models.py
│   ├── dag_build_dim_dealers_fictitious.py
│   └── dag_build_fact_sales.py
|
├── logs/                  # Logs gerados pelo Airflow (ignorado pelo .gitignore)
|
├── plugins/               # Para plugins customizados do Airflow (vazio neste projeto)
|
├── scripts/               # Scripts auxiliares, como o de popular a dim_date
│   └── populate_dim_date.py
|
├── .gitignore             # Arquivo que especifica o que o Git deve ignorar
├── docker-compose.yaml    # Arquivo principal que define e orquestra todos os serviços
├── Dockerfile             # Define a imagem customizada do Airflow com dependências extras
└── requirements.txt       # Lista de dependências Python a serem instaladas na imagem Docker
```

---

## 🚀 Como Executar o Projeto

Siga os passos abaixo para recriar e executar este ambiente.

### Pré-requisitos
* [Git](https://git-scm.com/)
* [Docker Desktop](https://www.docker.com/products/docker-desktop/)
* [WSL 2](https://learn.microsoft.com/pt-br/windows/wsl/install) instalado e configurado no Windows.

### Passos de Instalação

1.  **Clonar o Repositório**
    ```bash
    git clone [https://github.com/seu-usuario/pipeline-dados-airflow-mysql-powerbi.git](https://github.com/seu-usuario/pipeline-dados-airflow-mysql-powerbi.git)
    cd pipeline-dados-airflow-mysql-powerbi
    ```

2.  **Ajustar Permissões de Pastas**
    O Airflow no Docker precisa de permissões de escrita nas pastas de logs e plugins.
    ```bash
    sudo mkdir -p ./logs ./plugins
    sudo chown -R 50000:0 ./logs ./plugins
    ```

3.  **Construir e Iniciar os Contêineres**
    Este comando irá construir a imagem customizada do Airflow (com `Faker` instalado) e iniciar todos os serviços (Airflow, MySQL, Redis).
    ```bash
    docker-compose up -d --build
    ```
    Aguarde alguns minutos para que todos os serviços estejam no ar.

4.  **Configurar a Conexão no Airflow**
    * Acesse a interface do Airflow em `http://localhost:8080` (login: `admin`, senha: `admin`).
    * Vá em **Admin -> Connections** e crie a conexão para o banco de dados da aplicação:
        * **Connection Id:** `mysql_default`
        * **Connection Type:** `MySQL`
        * **Host:** `mysql_db`
        * **Schema:** `dados_api`
        * **Login:** `mysql_user`
        * **Password:** `mysql_pass`
        * **Port:** `3306`
    * Teste e salve a conexão.

5.  **Popular a Tabela de Datas**
    O pipeline depende de uma tabela de calendário (`dim_date`). Execute o script Python para populá-la uma única vez.
    ```bash
    # Instale as dependências no seu ambiente WSL primeiro
    python3 -m pip install pandas sqlalchemy mysql-connector-python

    # Execute o script
    python3 scripts/populate_dim_date.py
    ```
    
6.  **Executar as DAGs**
    * Na interface do Airflow, ative e execute as DAGs na seguinte ordem:
        1.  `build_dim_dealers_fictitious`
        2.  `build_dim_models`
        3.  `build_fact_sales` (ela esperará as outras duas terminarem)

---

## 📊 Análises no Power BI

Com o Data Mart populado, conecte o Power BI ao banco de dados `dados_api` (Host: `localhost`, Porta: `3306`, Usuário: `mysql_user`) para criar um dashboard interativo e responder perguntas como:
* Qual o faturamento total por marca e segmento?
* Como as vendas de veículos elétricos evoluíram ao longo dos anos?
* Qual o preço médio de venda para carros de luxo vs. carros de entrada?

---

## 👨‍💻 Autor

**[Seu Nome]**

* [LinkedIn](URL_DO_SEU_LINKEDIN)
* [GitHub](URL_DO_SEU_GITHUB)
