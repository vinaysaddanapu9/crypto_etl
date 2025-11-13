from setuptools import setup, find_packages

setup(
    name="crypto_etl",
    version="0.1.1",
    author="Vinay Saddanapu",
    author_email="your.email@example.com",
    description="PySpark ETL for CoinGecko data",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark>=3.3.0,<3.5.0",
        "pandas",
        "requests",
        # add other dependencies here
    ],
    entry_points={
        'console_scripts': [
            'run_crypto_etl=src.daily_etl:main',  # this lets you run the ETL from CLI
        ],
    },
    include_package_data=True,
    python_requires=">=3.7, <3.8",  # since you are on 3.7.x
)
