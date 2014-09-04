try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='mongoexport',
      version='0.0.1',
      url='https://github.com/samstav/mongoexport',
      py_modules=['mongoexport'],
      scripts=['mongoexport.py'],
      platforms = 'any',
     )
