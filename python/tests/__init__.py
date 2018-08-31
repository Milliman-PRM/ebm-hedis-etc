"""
## CODE OWNERS: Alexander Olivero, Jason Altieri
### OBJECTIVE:
  Mark the root of the tests folder so py.test will discover it properly.
### DEVELOPER NOTES:
  Once py.test discovers a test, it searches recusively up
    until it finds the last directory with a __init__.py.
  It then adds that directory to the `PYTHONPATH` before
    running the tests.
"""
