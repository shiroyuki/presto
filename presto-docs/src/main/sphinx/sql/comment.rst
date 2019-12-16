=======
COMMENT
=======

Synopsis
--------

.. code-block:: none

    COMMENT ON ( TABLE | VIEW ) name IS 'comments'

Description
-----------

Set the comment for a object. The comment can be removed by setting the comment to ``NULL``.

Examples
--------

Change the comment for the ``users`` table to be ``master table``::

    COMMENT ON TABLE users IS 'master table';

Change the comment for the ``users`` view to be ``master view``::

    COMMENT ON VIEW users IS 'master view';
