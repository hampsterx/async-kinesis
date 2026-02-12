from setuptools import setup

with open("README.md") as f:
    long_description = f.read()

setup(
    name="async-kinesis",
    description="AsyncIO Kinesis Library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version="1.1.5",
    url="https://github.com/hampsterx/async-kinesis",
    author="hampsterx",
    author_email="tim.vdh@gmail.com",
    license="Apache2",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
    ],
    python_requires=">=3.10",
    packages=["kinesis", "kinesis.cli"],
    install_requires=[
        "aiobotocore>=1.3.3",
        "async-timeout>=4.0.0",
        "asyncio-throttle>=1.0.0",
    ],
    extras_require={
        "kpl": ["aws-kinesis-agg>=1.1.6"],
        "redis": ["redis>=4.0.0"],
        "msgpack": ["msgpack>=0.6.1"],
        "prometheus": ["prometheus-client>=0.15.0"],
        "dynamodb": ["aioboto3>=11.0.0"],
        "testing": ["pytest>=7.0", "pytest-asyncio>=0.21"],
        "cli": ["click>=8.0.0"],
    },
    entry_points={
        # Registers kinesis.testing fixtures as a pytest plugin for all projects
        # with async-kinesis installed. Fixtures are no-ops if pytest is absent.
        "pytest11": ["kinesis = kinesis.testing"],
        "console_scripts": ["async-kinesis = kinesis.cli:main"],
    },
)
