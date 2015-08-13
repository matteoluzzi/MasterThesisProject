.. _administration-start:

===============================================================
Service start
===============================================================

Unless the start of the service is scripted, the simplest way to launch it is to run its single jar file and call the ``server`` command, as such:

.. parsed-literal::
    java -jar vimond-eventfetcher-service-|release|.jar server config.yml

Where ``config.yml`` is to be replaced by the path to the single configuration file of the service.