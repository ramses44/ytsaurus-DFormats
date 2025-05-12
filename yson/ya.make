LIBRARY()

SRCS(
    yson_types.h
    yson_types.cpp
    yson_reader.h
    yson_reader.cpp
    yson_writer.h
    yson_writer.cpp
)

PEERDIR(
    yt/yt/library/formats
    library/cpp/yson/node
    dformats/common
    dformats/interface
)

END()
