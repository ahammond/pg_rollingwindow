#!/usr/bin/env python

from setuptools import setup

setup(name='pg_rollingwindow',
      version='1.0',
      description='Manage partition tables in PostgreSQL',
      install_requires=['mock', 'patched_unittest', 'psycopg2', 'unittest2'],
      author='Andrew Hammond',
      author_email='andrew.george.hammond@gmail.com',
      url='https://github.com/ahammond/pg_rollingwindow',
      py_modules=['pg_rollingwindow'],
      scripts=['pg_rollingwindow.py'])