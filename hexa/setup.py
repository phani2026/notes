from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='hexa',
    author='Phaneendra',
    author_email='phani.2026@gmail.com',
    version='0.0.1',
    url='https://bphani.com',
    description='Hexa Car Python Project',
    long_description=long_description,
    packages=find_packages(include=['controller', 'controller.*']),
    python_requires='>=3.7',
    install_requires=[
        'sqlalchemy==1.3.15',
        'pyzmq==19.0.0',
        'uvicorn==0.11.3',
        'starlette==0.13.2',
        'fastapi==0.53.2',
        'pydantic==1.4',
        'python-multipart==0.0.5',
        'singleton_decorator==1.0.0',
        'cloudpickle>=1.3.0'
    ],
    classifiers=[
        'Development Status :: SNAPSHOT',
        'Framework :: Jupyter',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Software Development :: Libraries'
    ]
)