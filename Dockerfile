FROM public.ecr.aws/bitnami/python:3.9.5
ARG POETRY_VERSION=1.2.1
ENV PATH="/root/.local/bin:${PATH}"
RUN apt-get update \
    && apt-get install build-essential cmake -y \
    && apt-get clean \
    && pip install --upgrade pip \
    && pip install --user  --no-cache-dir poetry==${POETRY_VERSION} 


# working directory
WORKDIR . .
COPY . .
# create a volume to store the result
RUN mkdir /outputs
VOLUME /outputs
# initialize the poetry project
RUN poetry config virtualenvs.create false \ 
    && poetry run pip install --upgrade pip \ 
    && poetry add certifi \
    && poetry install --only=main

# from this point on build-essential and cmake are no longer required
RUN apt-get purge build-essential cmake -y 

# copy everything
ENTRYPOINT ["poetry", "run", "python", "main.py", "hydra.job.chdir=False"]
