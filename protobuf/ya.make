LIBRARY()

SRCS(
    protobuf_row_factory.h
    protobuf_row_factory.cpp
    protobuf_writer.h
    protobuf_writer.cpp
    protobuf_reader.h
    protobuf_reader.cpp
    protobuf_schema.h
    protobuf_schema.cpp
    protobuf_types.h
    protobuf_types.cpp
)

PEERDIR(
    yt/yt/library/formats
    yt/yt_proto/yt/formats

    dformats/interface
    dformats/common
)

END()
