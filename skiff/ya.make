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
)

PEERDIR(
    yt/yt/library/skiff_ext
    yt/yt/library/formats
    library/cpp/getopt

    dformats/interface
    dformats/common
)

END()
