FROM python:3.11.4

WORKDIR /app

# Install any dependencies your Python scripts might need
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python scripts into the container
COPY neo4J_conexion.py .
COPY conexionMongoChido.py .
COPY imdb_keys.csv .

# Command to run your Python scripts
CMD sleep 45 && python conexionMongoChido.py && \
    python neo4J_conexion.py
