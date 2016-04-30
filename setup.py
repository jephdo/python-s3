from setuptools import setup


setup(
    name='s3lib',
    version='0.1',
    packages=['s3lib'],
    py_modules=['cli'],
    entry_points="""
        [console_scripts]
        s3lib=cli:cli
    """,
)



