# Base Python 3.12 slim
FROM python:3.12-slim

# Diretório de trabalho
WORKDIR /app

# Instalar dependências do sistema (OpenJDK 21 e utilitários)
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk \
    poppler-utils \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Criar venv
RUN python -m venv /opt/venv

# Garantir que o venv seja usado
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Atualizar pip dentro do venv
RUN pip install --upgrade pip setuptools wheel

# Copiar requirements e instalar dependências Python no venv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código da aplicação e arquivos .jar
COPY . .

# Expor porta do app
EXPOSE 6969

# Comando para rodar a aplicação
CMD ["python", "app.py"]
