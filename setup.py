from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    docs = f.read()

with open("requirements.txt", "r", encoding="utf-8") as f:
    required = [
        line.strip()
        for line in f.read().splitlines()
        if line.strip() and not line.startswith("#")
    ]

setup(
    name="MI_ETLx",
    version="0.0.38",
    author=["Paschal Amah", "Nelson Ogbeide", "Joseph Ojo"],
    author_email="ojofemijoseph@gmail.com, ogbeide331@gmail.com, agaley.fesh@gmail.com",
    long_description=docs,
    long_description_content_type="text/markdown",
    url="https://github.com/ifebuche/oplog_works",
    download_url="https://github.com/ifebuche/oplog_works",
    packages=find_packages(exclude=["tests*", "docs*", "ms-data-pipeline*"]),
    install_requires=required,
    extras_require={
        "dynamo": ["boto3>=1.26.0"],
        "bigquery": ["google-cloud-bigquery>=3.0.0", "google-auth>=2.0.0"],
        "mongo": [],
        "notify": ["requests>=2.28.0"],
        "dev": ["python-dotenv>=0.21.0"],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    license="MIT",
    python_requires=">=3.8",
)
