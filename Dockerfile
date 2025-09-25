# Base Python 3.12 slim
FROM python:3.12-slim

# Diretório de trabalho
WORKDIR /app

# Instalar dependências do sistema (Wine + Java 21) e utilitários
RUN apt-get update && apt-get install -y \
    wine \
    openjdk-21-jre-headless \
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

# Copiar instalador Windows (opcional, se precisar do JSignPdf)
COPY JSignPdf_setup_2.3.0.exe /app/
RUN wine /app/JSignPdf_setup_2.3.0.exe /silent || true

# Copiar código da aplicação
COPY . .

# Expor porta do app
EXPOSE 3000

# Comando para rodar a aplicação
CMD ["python", "app.py"]
