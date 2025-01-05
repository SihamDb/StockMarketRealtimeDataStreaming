import os
from setuptools import setup, find_packages

ROOT = os.path.dirname(__file__)
print(f"ROOT -> {ROOT}")


setup(
    name="stockmarketrealtime",
    version="1.0.0",
    description="Real Time Stock Market Data",
    packages=find_packages(),
    include_package_data=True,
)
