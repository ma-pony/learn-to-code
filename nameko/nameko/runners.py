from __future__ import absolute_import

from contextlib import contextmanager
from logging import getLogger

from nameko.containers import get_container_cls, get_service_name
from nameko.utils.concurrency import SpawningProxy


_log = getLogger(__name__)


class ServiceRunner(object):
    """ Allows the user to serve a number of services concurrently.
    The caller can register a number of service classes with a name and
    then use the start method to serve them and the stop and kill methods
    to stop them. The wait method will block until all services have stopped.

    允许用户同时提供多种服务。
    调用者可以使用名称注册多个服务类，
    然后使用start方法为其提供服务，并使用stop和kill方法将其停止。
    wait方法将阻塞，直到所有服务都停止。

    Example::

        runner = ServiceRunner(config)
        runner.add_service(Foobar)
        runner.add_service(Spam)

        add_sig_term_handler(runner.kill)

        runner.start()

        runner.wait()
    """
    def __init__(self, config):
        self.service_map = {}
        self.config = config

        self.container_cls = get_container_cls(config)

    @property
    def service_names(self):
        return self.service_map.keys()

    @property
    def containers(self):
        return self.service_map.values()

    def add_service(self, cls):
        """ Add a service class to the runner.
        There can only be one service class for a given service name.
        Service classes must be registered before calling start()
        将服务类添加到runner。
        给定服务名称只能有一个服务类。
        必须在调用start()方法之前注册服务类
        """
        service_name = get_service_name(cls)
        container = self.container_cls(cls, self.config)
        self.service_map[service_name] = container

    def start(self):
        """ Start all the registered services.

        A new container is created for each service using the container
        class provided in the __init__ method.

        All containers are started concurrently and the method will block
        until all have completed their startup routine.

        启动所有注册的服务。
        使用__init__方法中提供的容器类为每个服务创建一个新容器。
        所有容器同时启动，该方法将阻塞，直到所有容器都完成了它们的启动例程。
        """
        service_names = ', '.join(self.service_names)
        _log.info('starting services: %s', service_names)

        SpawningProxy(self.containers).start()

        _log.debug('services started: %s', service_names)

    def stop(self):
        """ Stop all running containers concurrently.
        The method blocks until all containers have stopped.

        同时停止所有正在运行的容器。
        该方法将阻塞，直到所有容器停止。
        """
        service_names = ', '.join(self.service_names)
        _log.info('stopping services: %s', service_names)

        SpawningProxy(self.containers).stop()

        _log.debug('services stopped: %s', service_names)

    def kill(self):
        """ Kill all running containers concurrently.
        The method will block until all containers have stopped.
        杀死所有正在运行的容器
        该方法将阻塞，直到所有容器停止
        """
        service_names = ', '.join(self.service_names)
        _log.info('killing services: %s', service_names)

        SpawningProxy(self.containers).kill()

        _log.debug('services killed: %s ', service_names)

    def wait(self):
        """ Wait for all running containers to stop.
        等待所有正在运行的容器停止
        """
        try:
            SpawningProxy(self.containers, abort_on_error=True).wait()
        except Exception:
            # If a single container failed, stop its peers and re-raise the
            # exception
            self.stop()
            raise


@contextmanager
def run_services(config, *services, **kwargs):
    """ Serves a number of services for a contextual block.
    The caller can specify a number of service classes then serve them either
    stopping (default) or killing them on exiting the contextual block.


    Example::

        with run_services(config, Foobar, Spam) as runner:
            # interact with services and stop them on exiting the block

        # services stopped


    Additional configuration available to :class:``ServiceRunner`` instances
    can be specified through keyword arguments::

        with run_services(config, Foobar, Spam, kill_on_exit=True):
            # interact with services

        # services killed

    :Parameters:
        config : dict
            Configuration to instantiate the service containers with
        services : service definitions
            Services to be served for the contextual block
        kill_on_exit : bool (default=False)
            If ``True``, run ``kill()`` on the service containers when exiting
            the contextual block. Otherwise ``stop()`` will be called on the
            service containers on exiting the block.

    :Returns: The configured :class:`ServiceRunner` instance

    """
    kill_on_exit = kwargs.pop('kill_on_exit', False)

    runner = ServiceRunner(config)
    for service in services:
        runner.add_service(service)

    runner.start()

    yield runner

    if kill_on_exit:
        runner.kill()
    else:
        runner.stop()
