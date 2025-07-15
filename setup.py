from setuptools import find_packages, setup

with open("README.md", "r") as f:
    long_description = f.read()


setup(
    name="lakehouselib",
    version="1.2.8",
    description="This library interacts with the data lakehouse infrastructure",
    package_dir={"": "app"},
    packages=find_packages(where="app"),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/danilo-dcs/lakehouse_infra",
    author="Danilo Silva",
    author_email="danilo.dcs09@gmail.com",
    license="MIT",
    license_files=("LICENSE.txt"),  # Add this line to include the license file
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent"
    ],
    install_requires=[
        "pandas>=2.2.2",
        "openpyxl>=3.1.5",
        "html5lib>=1.1",
        "pyarrow>=14.0.0",
        "fastparquet>=2024.11.0",
        "requests>=2.32.3",
        "pydantic>=2.11.4"
    ],
    extras_require={
        "dev": ["pytest>=7.0", "twine>=4.0.2"]
    },
    python_requires=">=3.9",
    include_package_data=True,
)