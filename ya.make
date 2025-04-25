LIBRARY()

SRCS(
    skiff_reader.h
    skiff_reader.cpp
    skiff_writer.h
    skiff_writer.cpp
    skiff_types.h
    skiff_types.cpp
    skiff_schema.h
    skiff_schema.cpp
    arrow_adapter.h
    arrow_reader.h
    arrow_reader.cpp
    arrow_writer.h
    arrow_writer.cpp
    reading_context.h
)

PEERDIR(
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/io
    yt/yt/library/process
    yt/yt/library/skiff_ext
    yt/yt/client
    yt/yt/library/formats
    library/cpp/getopt
    contrib/libs/apache/arrow
)

END()

RECURSE(
  benchmarks
  examples
)

RECURSE_FOR_TESTS(
  unittests
)
