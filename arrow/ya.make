LIBRARY()

SRCS(
    arrow_adapter.h
    arrow_schema.h
    arrow_types.h
    arrow_types.cpp
    arrow_reader.h
    arrow_reader.cpp
    arrow_writer.h
    arrow_writer.cpp
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
    yt/yt_proto/yt/formats

    dformats/interface
    dformats/yson
)

END()
