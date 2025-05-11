#pragma once

#include <util/generic/fwd.h>
#include <util/generic/hash.h>
#include <util/generic/yexception.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include <yt/yt_proto/yt/formats/extension.pb.h>

#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>

using namespace google::protobuf;

namespace DFormats {

static constexpr char kExtensionProtoFile[] = "yt/yt_proto/yt/formats/extension.proto";
static constexpr char kDynamicProtoFile[] = "dynamic.proto";

DescriptorProto* MakeProtoStruct(FileDescriptorProto* fileProto, NTi::TStructTypePtr structType, TStringBuf name);

DescriptorProto* MakeMapEntry(FileDescriptorProto* fileProto, NTi::TDictTypePtr mapType, TStringBuf name);
DescriptorProto* MakeProtoVariant(FileDescriptorProto* fileProto, NTi::TVariantTypePtr variantType, TStringBuf name, TStringBuf oneofFieldName);

void InitFieldType(FileDescriptorProto* fileProto, FieldDescriptorProto* fieldProto, NTi::TTypePtr type, TStringBuf parentMsgName = {});

DescriptorPool* MakeDescriptorPool(const THashMap<TString, NYT::TTableSchema>& msgSchemas);

}
