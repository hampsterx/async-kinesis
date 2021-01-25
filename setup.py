from setuptools import setup

with open("README.md") as f:
    long_description = f.read()

setup(
    name="async-kinesis",
    description="AsyncIO Kinesis Library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version="1.1.2",
    url="https://github.com/hampsterx/async-kinesis",
    author="hampsterx",
    author_email="tim.vdh@gmail.com",
    license="Apache2",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
    ],
    packages=["kinesis"],
    install_requires=[
        "aiobotocore>=1.0.4",
        "async-timeout>=3.0.1",
        "asyncio-throttle>=0.1.1",
    ],
    extras_require={
        "kpl": ["aws-kinesis-agg>=1.1.6"],
        "redis": ["aredis>=1.1.8"],
        "msgpack": ["msgpack>=0.6.1"],
    },
)
