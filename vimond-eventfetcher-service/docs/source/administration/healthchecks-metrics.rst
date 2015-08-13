.. _administration-healthchecks-metrics:

===============================================================
Healthchecks and metrics
===============================================================

The service provides healthchecks and metrics that can be accessed on the :ref:`admin port <administration-overview>` of the service.
They can be accessed via the following resources: ``/healthcheck`` and ``/metrics``. The healtchecks return a status (``OK`` or ``KO``) that enable the
operator to verify that the service runs properly. The metrics measure the behaviour of the system and its usage. Healthchecks and metrics are not
further described here, as their actual contents will probably change soon to improve their coverage. Please contact our team for more information.
A ``/ping`` resource is also available on that admin port.