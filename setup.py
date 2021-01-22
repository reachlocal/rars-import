from setuptools import setup, find_packages


setup(name='rars_import',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'google-api-python-client==1.10.0',
        'google-auth==1.21.1',
        'google-cloud-bigquery==1.26.1',
        'google-cloud-storage==1.30.0',
        'mysqlclient==2.0.2'
    ]
)