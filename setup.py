from setuptools import setup, find_packages

def readme():
    with open('README.rst') as f:
        return f.read()

setup(name = "dasksge",
    version = 0.1,
    description = "deploy Dask.distributed to GridEngine (sge/pbs) cluster systems",
    long_description = readme(),
    author = "Morgan Fouesneau",
    author_email = "",
    url = "https://github.com/mfouesneau/dasksge",
    packages = find_packages(),
    classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Science/Research',
      'Operating System :: OS Independent',
      'Programming Language :: Python',
      'Topic :: Scientific/Engineering'
      ],
    zip_safe=False
)
