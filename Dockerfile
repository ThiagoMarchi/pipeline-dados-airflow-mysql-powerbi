# Usa a imagem oficial do Airflow como base
FROM apache/airflow:2.9.2

# Define a URL do arquivo de constraints como um argumento
# IMPORTANTE: Ajuste a versão do Python se necessário (ex: 3.8, 3.9, etc.)
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.12.txt"

# Copia o nosso arquivo de dependências para dentro da imagem
COPY requirements.txt /

# Instala as dependências usando o arquivo de constraints para garantir a compatibilidade
# Este método é mais seguro e evita erros de compilação
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"
