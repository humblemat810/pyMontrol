from setuptools import setup, find_packages

setup(
    name='pymontroller',
    version='0.0.0',
    packages=['pymontroller', 'pymontroller.process_data_local'],
    install_requires = ['PyYAML']
)