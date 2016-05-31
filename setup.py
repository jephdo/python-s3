from setuptools import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()


setup(
    name='s3lib',
    version='0.1',
    packages=['s3lib'],
    py_modules=['cli'],
    install_requires=required,
    entry_points="""
        [console_scripts]
        s3lib=cli:cli
    """,
)



