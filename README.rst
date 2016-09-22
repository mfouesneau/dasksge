"""
Dask on GridEngine system
=========================

Deploy a pool of Dask workers through SGE/PBS system using the `drmaa` library.

This package extends `distributed`_, a lightweight library for distributed
computing in Python, which  exports `dask`_ APIs to moderate sized clusters.
One signle object will start scheduler and workers and will also takes care of
stopping them once the work is done.

The default behavior is to run as many workers as requested with 1 thread each.
The main class takes care of starting a scheduler similar to
`distributed.LocalCluster` (which includes web monitor for instance) and submit
an array of jobs to the SGE/PBS system to start `dask-worker` processes.

Note that the workers are using the command line `dask-worker` for simplicity.


Examples
--------

A first example could be

.. code::

    sge = GridEngineScheduler()
    sge.start_pool(10)
    # ... work ...
    sge.stop_pool()

But the provided scheduler can also be used as context manager:

.. code::

    with GridEngineScheduler(nworkers=10) as sge:
        # ... work ....


This work was inspired by Matthew Rocklin's `blog post`_

.. dask : https://dask.readthedocs.io/en/latest/
.. distributed : https://distributed.readthedocs.io/en/latest/
.. blog post : http://matthewrocklin.com/blog/work/2016/02/26/dask-distributed-part-3
