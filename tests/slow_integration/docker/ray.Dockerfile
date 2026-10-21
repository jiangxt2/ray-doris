ARG RAY_BASE_IMAGE=ray-doris-ray-base:2.59.0-py312
FROM ${RAY_BASE_IMAGE}
ARG UV_DEFAULT_INDEX=https://pypi.org/simple

USER root
WORKDIR /workspace

RUN python -c 'import ray; assert ray.__version__ == "2.59.0", ray.__version__' \
    && openssl version

COPY pyproject.toml README.md LICENSE NOTICE ./
COPY src ./src
COPY tests/slow_integration ./tests/slow_integration
COPY tests/__init__.py tests/projection_helpers.py ./tests/

RUN --mount=type=cache,target=/var/cache/uv \
    UV_CACHE_DIR=/var/cache/uv UV_DEFAULT_INDEX=${UV_DEFAULT_INDEX} \
    uv pip install --system ".[test,flight]"

ENV PYTHONUNBUFFERED=1
USER ray
