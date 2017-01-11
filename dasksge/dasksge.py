"""
Deploy a pool of Dask workers through SGE/PBS system using the `drmaa` library.

One single object will start scheduler and workers and will also takes care of
stopping them once the work is done.

The default behavior is to run as many workers as requested with 1 thread each.
The main class takes care of starting a scheduler similar to
:class:`distributed.LocalCluster` (which includes web monitor for instance) and submit an
array of jobs to the SGE/PBS system to start `dask-worker` processes.

Note that the workers are using the command line `dask-worker` for simplicity.

.. example::

    sge = GridEngineScheduler()
    sge.start_pool(10)
    # ... work ...
    sge.stop_pool()


This work was inspired by `Matthew Rocklin's blog post`_

.. Matthew Rocklin's blog post: http://matthewrocklin.com/blog/work/2016/02/26/dask-distributed-part-3
"""
from __future__ import print_function
import os
import socket
import logging
import distributed
from distributed.http import HTTPScheduler
from tornado.ioloop import IOLoop
from threading import Thread
from time import sleep

__all__ = ['GridEngineScheduler']

# ----------------------------------------------------------------------------
# General
# ----------------------------------------------------------------------------
script = """
#!/bin/bash
# GridEngine qsub submission script for Dask worker
# invoke the client
dask-worker --nthreads 1 {host:s}:{port:d}
"""

CURDIR = os.getcwd()
HOSTNAME = socket.gethostname()
HOSTIP = socket.gethostbyname(HOSTNAME)

# ----------------------------------------------------------------------------
# Default Resources
# ----------------------------------------------------------------------------
try:
    import drmaa
    if drmaa.Session.drmsInfo == 'PBS Professional':
        # PBS/Torque Scheduler
        DEFAULT_RESOURCES = {}
    else:
        # Sun GridEngine Scheduler
        DEFAULT_RESOURCES = {}
except:
    # Process Scheduler
    DEFAULT_RESOURCES = {}


# ----------------------------------------------------------------------------
# Generic Dask LocalCluster -- Keep interface similar
# ----------------------------------------------------------------------------
class _Cluster(distributed.LocalCluster):
    """
    Create local Scheduler and Workers
    This creates a "cluster" of a scheduler and workers running on the local
    machine, for which it's listening ip is flexible.

    This class is derived from the main distributed :class:`LocalCluster` to
    keep all services provided with it. It's default behavior is only to start the
    scheduler

    Parameters
    ----------
    n_workers: int
        Number of workers to start

    threads_per_worker: int
        Number of threads per each worker

    nanny: boolean
        If true start the workers in separate processes managed by a nanny.
        If False keep the workers in the main calling process

    scheduler_port: int
        Port of the scheduler.  8786 by default, use 0 to choose a random port
    """
    def __init__(self, n_workers=0, threads_per_worker=1, nanny=True,
                 loop=None, start=True, scheduler_port=8786, scheduler_ip=HOSTIP,
                 silence_logs=logging.CRITICAL, diagnostics_port=8787,
                 services={'http': HTTPScheduler}, **kwargs):
        self.status = None
        self.nanny = nanny

        if silence_logs:
            for l in ['distributed.scheduler', 'distributed.worker',
                      'distributed.core', 'distributed.nanny']:
                logging.getLogger(l).setLevel(silence_logs)

        self.loop = loop or IOLoop()
        if not self.loop._running:
            self._thread = Thread(target=self.loop.start)
            self._thread.daemon = True
            self._thread.start()
            while not self.loop._running:
                sleep(0.001)

        self.scheduler = distributed.Scheduler(loop=self.loop, ip=HOSTIP,
                                               services=services)
        self.scheduler.start(scheduler_port)
        self.workers = []

        if start:
            _start_worker = self.start_worker
        else:
            _start_worker = lambda *args, **kwargs: self.loop.add_callback(self._start_worker, *args, **kwargs)
        for i in range(n_workers):
            _start_worker(ncores=threads_per_worker, nanny=nanny)
        self.status = 'running'

        self.diagnostics = None
        if diagnostics_port is not None:
            self.start_diagnostics_server(diagnostics_port, silence=silence_logs)


# ----------------------------------------------------------------------------
# Grid Engine Scheduler
# ----------------------------------------------------------------------------
class GridEngineScheduler(object):
    """
    A Scheduler that schedules jobs on a Sun Grid Engine (SGE) using the drmaa
    library
    """
    def __init__(self, nworkers=1, **resources):
        """Initialize a GridEngineScheduler instance

        Only one instance may run per Python process, since the underlying drmaa
        layer is a singleton.

        Parameters
        ----------
        nworkers: int
            number of workers (can be updated later)

        Resources: dict
            to be passed to the -l command of qsub.
            e.g.:
                h_cpu: maximum time expressed in format '02:00:00' (2 hours)
                h_vmem: maximum memory allocation before job is killed in format '10G' (10GB)
                virtual_free: memory free on host BEFORE job can be allocated
        """
        import drmaa
        self.drmaa = drmaa
        self.nworkers = nworkers

        # pass-through options to the jobs
        self.resources = DEFAULT_RESOURCES
        self.resources.update(resources)
        self.session = drmaa.Session()
        self.session.initialize()
        self.sgeids = []
        self.address = HOSTNAME
        self.cluster = None

    def __del__(self):
        if hasattr(self, 'drmaa'):
            try:
                self.killall()
                self.session.exit()
            except (TypeError, self.drmaa.errors.NoActiveSessionException):
                pass

    def _schedule(self, njobs, **resources):
        """schedule the jobs (dict of {jobid, job.Job}) to run

        Parameters
        ----------
        job_queue: dict
            the dict of {jobid, job.Job} items to run

        Resources: dict
            to be passed to the -l command of qsub. These override any
            arguments that were given to the constructor. e.g.
                h_cpu: maximum time expressed in format '02:00:00' (2 hours)
                h_vmem: maximum memory allocation before job is killed in format '10G' (10GB)
                virtual_free: memory free on host BEFORE job can be allocated
        """
        # update the keyword resources
        try:
            resources = dict(self.resources.items() + resources.items())
        except:
            resources = dict(tuple(self.resources.items()) + tuple(resources.items()))

        WRAPPER = CURDIR + '/dask-worker.jobscript'
        LOGGER  = CURDIR + '/dask-worker.log'

        # Quite stupid but getting sometimes BlockingIOError
        IOissue = True
        while(IOissue):
            try:
                with open(WRAPPER, 'w') as f:
                    f.write(script.format(host=HOSTNAME, port=8786))
                IOissue = False
            except:
                IOissue = True

        # build the homogenous job template and submit array
        with self.session.createJobTemplate() as jt:
            jt.jobEnvironment = os.environ

            jt.remoteCommand = os.path.expanduser(WRAPPER)
            # jt.args = [submission_host]
            jt.jobName = resources.pop('name','dask-worker')
            jt.jobName = ''.join(jt.jobName.split())[:15]
            jt.nativeSpecification = '-l ' + ','.join(
                resource + '=' + str(value) for resource,value in resources.items()
            ) if resources else ''
            jt.joinFiles = True
            jt.outputPath = ':' + os.path.expanduser(LOGGER)
            jt.errorPath  = ':' + os.path.expanduser(LOGGER)
            jt.WORKING_DIRECTORY = os.getcwd()
            jt.workingDirectory = os.getcwd()

            self.sgeids  = self.session.runBulkJobs(jt, 1, njobs, 1)
            self.arrayid = self.sgeids[0].split('.')[0]
            print('GridEngineScheduler: submitted {0} jobs in array {1}'
                  .format(njobs, self.arrayid))

    def __str__(self):
        sch_addr = HOSTIP
        sch_nworkers = 0
        sch_ncores = 0
        sch_status = "not running"
        if self.cluster is not None:
            sch = self.cluster.scheduler
            sch_addr = self.cluster.scheduler_address
            sch_nworkers = len(sch.ncores)
            sch_ncores = sum(sch.ncores.values())
            sch_status = self.cluster.status
        txt = """SGE DASK-Cluster("{0:s}", status="{3:s}", workers={1:d}, ncores={2:d})"""
        return txt.format(sch_addr, sch_nworkers, sch_ncores, sch_status)

    def __repr__(self):
        return self.__str__()

    def start_dask_scheduler(self, **kwargs):
        """
        Create local Scheduler and optional workers
        This creates a "cluster" of a scheduler and workers running on the local
        machine, for which it's listening ip is flexible.

        This method is similar to creating an object from
        :class:`LocalCluster`. Therefore, it provides all services provided
        with it. It's default behavior is only to start the scheduler on the non-internal
        ip-loop

        Parameters
        ----------
        n_workers: int
            Number of workers to start

        threads_per_worker: int
            Number of threads per each worker

        nanny: boolean
            If true start the workers in separate processes managed by a nanny.
            If False keep the workers in the main calling process

        scheduler_port: int
            Port of the scheduler.  8786 by default, use 0 to choose a random port
        """
        if self.cluster is not None:
            self.cluster.close()
        self.cluster = _Cluster(**kwargs)

    def killall(self, verbose=False):
        """Terminate any running jobs"""
        self.session.control(self.drmaa.Session.JOB_IDS_SESSION_ALL,
                             self.drmaa.JobControlAction.TERMINATE)

    def start_pool(self, n, **kwargs):
        """ Start a pool of workers

        Parameters
        ----------
        n:int
            number of workers to put in the queue
        """
        self.start_dask_scheduler(**kwargs)
        self._schedule(n)

    def stop_pool(self):
        """ Stop the pool. Request workers to be killed """
        self.killall()

    def close(self):
        """ Close scheduler and workers """
        self.killall()
        self.cluster.close()

    def get_jobids(self):
        """ returns the job ids of each worker job """
        return [k for k in self.sgeids]

    def get_executor(self, **kwargs):
        """ Return an associated executor """
        return distributed.Executor(self.cluster.scheduler_address, **kwargs)

    # context manager
    def __enter__(self):
        if self.nworkers <= 1:
            raise RuntimeError("number of workers is expected to be >1")
        self.start_pool(self.nworkers)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
