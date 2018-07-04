from setuptools import setup

VERSION = '1.0.0a1.dev4'
DESCRIPTION = """
Input and Output wishbone modules for Couchdb
"""
CLASSIFIERS = [
    'Development Status :: 2 - Pre-Alpha',
    'Programming Language :: Python',
    # 'Programming Language :: Python :: 2',
    # 'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
]
INSTALL_REQUIRES = [
    'setuptools',
    'wishbone',
    'ujson',
    'CouchDB',
    'gevent',
    'jq'
]
TEST_REQUIRES = [
    'pytest',
    'pytest-cov',
    'mock'
]
EXTRA = {
    "test": TEST_REQUIRES
}
ENTRY_POINTS = {
    'wishbone.module.input': [
        'couchdb = wishbonecouchdb:CouchdbPoller'
    ],
    'wishbone.module.output': [
        'couchdb = wishbonecouchdb:CouchdbPush'
    ],
    'wishbone.module.flow': [
        'viewfilter = wishbonecouchdb:ViewFilter',
        'jqfilter = wishbonecouchdb:JQFilter'
    ],
}

setup(name='wishbonecouchdb',
      version=VERSION,
      description=DESCRIPTION,
      author='Quintagroup, Ltd.',
      author_email='info@quintagroup.com',
      license='Apache License 2.0',
      classifiers=CLASSIFIERS,
      include_package_data=True,
      packages=['wishbonecouchdb'],
      zip_safe=False,
      install_requires=INSTALL_REQUIRES,
      extras_require=EXTRA,
      entry_points=ENTRY_POINTS
      )
