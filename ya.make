LIBRARY()

PEERDIR(
  dformats/mapreduce
)

END()

RECURSE(
  benchmarks
  examples
)

RECURSE_FOR_TESTS(
  unittests
)
