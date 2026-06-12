"""TEMP leak canary — DO NOT MERGE. Delete once the ASAN+LSAN validation run
is read (paired with print_suppressions=1 + the extra pytest arg in
.semaphore/semaphore.yml).

Both tests pass functionally, so the only thing that can turn the job red is
LSAN. The artifact log then answers the two things a green run can't:

  - test_canary_raw_malloc_leak: a pure libc block, no Python allocator frame
    for any rule to match. It MUST be reported and MUST flip the job to exit 1.
    If it isn't, the detector or the exitcode=1 gate is broken.

  - test_canary_leaked_gc_object: a lost GC object whose alloc stack runs
    through _PyObject_GC_* — exactly the broad frames our suppression list
    carries. If LSAN stays silent on this one (and print_suppressions=1 shows a
    _PyObject_GC_* rule firing), the suppressions are masking our own objects.
"""
import ctypes


def test_canary_raw_malloc_leak():
    libc = ctypes.CDLL(None)
    libc.malloc.restype = ctypes.c_void_p
    libc.malloc.argtypes = [ctypes.c_size_t]
    libc.malloc(1234567)  # distinctive size; return discarded, never freed


def test_canary_leaked_gc_object():
    obj = {"leak_canary": 7654321}                     # dict -> GC-tracked
    ctypes.pythonapi.Py_IncRef(ctypes.py_object(obj))  # pin refcount forever
    del obj                                            # drop the only referrer
