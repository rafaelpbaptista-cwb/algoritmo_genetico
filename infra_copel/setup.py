# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

import versioneer

setup(
      name='infra_copel',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      author="COPEL",
      packages=find_packages(),
      url="https://gitprd.copel.nt/cppc/infra_copel.git",
      python_requires=">=3.9",
      install_requires=["pandas>=1.4", "pymongo", "lxml", "openpyxl", "requests_toolbelt", "xlcalculator", "pydantic", "polars"],
)
