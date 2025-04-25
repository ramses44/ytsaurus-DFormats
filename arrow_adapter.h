#pragma once

#include <util/stream/input.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>

class TArrowInputStreamAdapter : public arrow::io::InputStream {  // source: yt/yt/python/yson/arrow/file_format_converters.cpp:247
public:
    explicit TArrowInputStreamAdapter(IInputStream* stream) : Stream_(stream) {
        IsClosed_ = !Stream_->ReadChar(PreviousElement_);
    }

    arrow::Status Close() override {
        return arrow::Status::OK();
    }

    bool closed() const override {
        return IsClosed_;
    }

    arrow::Result<int64_t> Tell() const override {
        return Position_;
    }

    arrow::Result<int64_t> Read(int64_t nBytes, void* out) override {
        return DoLoad(out, nBytes);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nBytes) override {
        std::string buffer;
        buffer.resize(nBytes);
        buffer.resize(DoLoad(buffer.data(), buffer.size()));
        return arrow::Buffer::FromString(buffer);
    }

private:
    int64_t Position_ = 0;
    bool IsClosed_ = false;
    char PreviousElement_;
    IInputStream* Stream_;

    size_t DoLoad(void* buf, size_t len) {
        if (IsClosed_ || len == 0) {
            return 0;
        }
        char* outChar = reinterpret_cast<char*>(buf);
        *outChar = PreviousElement_;
        outChar++;
        auto nBytes = Stream_->Load(outChar, len - 1) + 1;
        Position_ += nBytes;
        IsClosed_ = !Stream_->ReadChar(PreviousElement_);
        return nBytes;
    }
};

class TArrowOutputStreamAdapter : public arrow::io::OutputStream {
public:
    explicit TArrowOutputStreamAdapter(IOutputStream* stream) 
        : Stream_(stream), 
          Position_(0),
          IsClosed_(false) 
    {
    }

    arrow::Status Close() override {
        if (!IsClosed_) {
            Stream_->Finish();
            IsClosed_ = true;
        }
        return arrow::Status::OK();
    }

    bool closed() const override { 
        return IsClosed_; 
    }

    arrow::Result<int64_t> Tell() const override {
        return Position_;
    }

    arrow::Status Write(const void* data, int64_t nbytes) override {
        if (IsClosed_) {
            return arrow::Status::IOError("Stream closed");
        }
        
        if (nbytes > 0) {
            Stream_->Write(data, static_cast<size_t>(nbytes));
            Position_ += nbytes;
        }
        return arrow::Status::OK();
    }

    arrow::Status Flush() override {
        Stream_->Flush();
        return arrow::Status::OK();
    }

private:
    IOutputStream* Stream_;
    int64_t Position_;
    bool IsClosed_;
};