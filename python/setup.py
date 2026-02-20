from setuptools import setup, find_packages

setup(
    name="kvstore-client",
    version="0.1.0",
    description="Python client for the kv-store Next.js API",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "requests>=2.28.0",
    ],
    extras_require={
        "rag": [
            "numpy>=1.24.0",
            "sentence-transformers>=2.7.0",
        ],
    },
)
