FROM python:3.12

# Install linux stuff
RUN apt update; apt install -y less git nano dcmtk iputils-ping
    
# Install uv
RUN pip install uv

# Copy project files from repo root
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen

ENTRYPOINT ["/bin/bash"]
