from setuptools import find_packages, setup

try:
    README = open('README.rst').read()
except IOError:
    README = None

setup(
    name='guillotina_statsd',
    version='1.0.2',
    description='Integrate statsd into guillotina',
    long_description=README,
    install_requires=[
        'guillotina',
        'aiostatsd'
    ],
    author='Nathan Van Gheem',
    author_email='vangheem@gmail.com',
    url='',
    packages=find_packages(exclude=['demo']),
    include_package_data=True,
    tests_require=[
        'pytest',
    ],
    extras_require={
        'test': [
            'pytest'
        ]
    },
    classifiers=[],
    entry_points={
    }
)
