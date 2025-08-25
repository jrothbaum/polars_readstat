/**
 * \file include/cppsas7bdat/sink/arrow.hpp
 *
 * \brief Apache Arrow datasink for streaming chunked output with character encoding support
 *
 * \author Modified for streaming based on cppsas7bdat CSV sink
 */
#ifndef _CPP_SAS7BDAT_SINK_ARROW_HPP_
#define _CPP_SAS7BDAT_SINK_ARROW_HPP_

#include <cppsas7bdat/column.hpp>
#include <arrow/api.h>
#include <arrow/record_batch.h>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/type.h>
#include <arrow/status.h>
#include <arrow/c/bridge.h>  // For C Data Interface
#include <arrow/util/utf8.h>  // Arrow UTF-8 utilities
#ifdef _WIN32
// On Windows, declare iconv functions as external (they'll be provided by iconv-sys via Rust)
extern "C" {
    typedef void* iconv_t;
    iconv_t libiconv_open(const char* tocode, const char* fromcode);
    size_t libiconv(iconv_t cd, char** inbuf, size_t* inbytesleft, char** outbuf, size_t* outbytesleft);
    int libiconv_close(iconv_t cd);
    
    // Create aliases for the standard names
    #define iconv_open libiconv_open
    #define iconv libiconv
    #define iconv_close libiconv_close
}
#define HAVE_ICONV 1
#else
#include <iconv.h>
#endif
#include <errno.h>
#include <memory>
#include <vector>
#include <string>
#include <cstring>

// Include the cppsas7bdat encoding detection
namespace cppsas7bdat {
namespace INTERNAL {
std::string_view get_encoding(const uint8_t _e) noexcept;
} // namespace INTERNAL
} // namespace cppsas7bdat

namespace cppsas7bdat {
namespace datasink {
namespace detail {

// Optimized character conversion utility class with buffer reuse
class charset_converter {
private:
    iconv_t converter_;
    bool is_valid_;
    std::string from_encoding_;
    std::string to_encoding_;
    
    // Pre-allocated buffer for conversions - reused across calls
    mutable std::string conversion_buffer_;
    
public:
    charset_converter(const std::string& from_encoding = "", const std::string& to_encoding = "UTF-8") 
        : converter_(reinterpret_cast<iconv_t>(-1)), is_valid_(false), 
          from_encoding_(from_encoding), to_encoding_(to_encoding) {
        
        if (!from_encoding.empty() && from_encoding != to_encoding) {
            converter_ = iconv_open(to_encoding.c_str(), from_encoding.c_str());
            if (converter_ != reinterpret_cast<iconv_t>(-1)) {
                is_valid_ = true;
                // Pre-allocate conversion buffer to avoid repeated allocations
                conversion_buffer_.reserve(8192); // 8KB default
            }
        }
    }
    
    ~charset_converter() {
        if (is_valid_ && converter_ != reinterpret_cast<iconv_t>(-1)) {
            iconv_close(converter_);
        }
    }
    
    // Non-copyable
    charset_converter(const charset_converter&) = delete;
    charset_converter& operator=(const charset_converter&) = delete;
    
    // Movable
    charset_converter(charset_converter&& other) noexcept 
        : converter_(other.converter_), is_valid_(other.is_valid_),
          from_encoding_(std::move(other.from_encoding_)), to_encoding_(std::move(other.to_encoding_)),
          conversion_buffer_(std::move(other.conversion_buffer_)) {
        other.converter_ = reinterpret_cast<iconv_t>(-1);
        other.is_valid_ = false;
    }
    
    charset_converter& operator=(charset_converter&& other) noexcept {
        if (this != &other) {
            if (is_valid_ && converter_ != reinterpret_cast<iconv_t>(-1)) {
                iconv_close(converter_);
            }
            converter_ = other.converter_;
            is_valid_ = other.is_valid_;
            from_encoding_ = std::move(other.from_encoding_);
            to_encoding_ = std::move(other.to_encoding_);
            conversion_buffer_ = std::move(other.conversion_buffer_);
            other.converter_ = reinterpret_cast<iconv_t>(-1);
            other.is_valid_ = false;
        }
        return *this;
    }
    
    enum convert_result {
        CONVERT_OK = 0,
        CONVERT_LONG_STRING,
        CONVERT_BAD_STRING,
        CONVERT_ERROR
    };
    
    // Optimized batch conversion for Arrow string arrays
    arrow::Result<std::shared_ptr<arrow::Array>> convert_string_array_batch(
        const std::shared_ptr<arrow::Array>& input_array) const {
        
        if (!is_valid_) {
            return input_array; // No conversion needed
        }
        
        auto string_array = std::static_pointer_cast<arrow::StringArray>(input_array);
        auto pool = arrow::default_memory_pool();
        arrow::StringBuilder builder(pool);
        
        // Pre-allocate based on input size with padding for encoding expansion
        ARROW_RETURN_NOT_OK(builder.Reserve(string_array->length()));
        
        // Estimate total data size (UTF-8 can be up to 4x larger than source)
        int64_t estimated_data_size = 0;
        for (int64_t i = 0; i < string_array->length(); ++i) {
            if (!string_array->IsNull(i)) {
                estimated_data_size += string_array->value_length(i) * 4; // 4x expansion estimate
            }
        }
        ARROW_RETURN_NOT_OK(builder.ReserveData(estimated_data_size));
        
        // Reset converter state once at the beginning of the batch
        iconv(converter_, nullptr, nullptr, nullptr, nullptr);
        
        // Process all strings in the batch
        for (int64_t i = 0; i < string_array->length(); ++i) {
            if (string_array->IsNull(i)) {
                ARROW_RETURN_NOT_OK(builder.AppendNull());
                continue;
            }
            
            auto value = string_array->GetString(i);
            std::string converted;
            auto result = convert_string_optimized(converted, value.data(), value.length());
            
            if (result == CONVERT_OK) {
                ARROW_RETURN_NOT_OK(builder.Append(converted));
            } else {
                // Fallback to original on conversion failure
                ARROW_RETURN_NOT_OK(builder.Append(value));
            }
        }
        
        std::shared_ptr<arrow::Array> converted_array;
        ARROW_RETURN_NOT_OK(builder.Finish(&converted_array));
        return converted_array;
    }
    
private:
    convert_result convert_string_optimized(std::string& dst, const char* src, size_t src_len) const {
        if (!is_valid_ || !src || src_len == 0) {
            dst.assign(src, src_len);
            return CONVERT_OK;
        }
        
        // Strip trailing spaces and nulls efficiently
        while (src_len && (src[src_len-1] == ' ' || src[src_len-1] == '\0')) {
            src_len--;
        }
        
        if (src_len == 0) {
            dst.clear();
            return CONVERT_OK;
        }
        
        // Use pre-allocated buffer, resize if needed
        size_t estimated_size = src_len * 4 + 1; // UTF-8 expansion estimate
        if (conversion_buffer_.capacity() < estimated_size) {
            conversion_buffer_.reserve(estimated_size * 2); // Extra headroom
        }
        conversion_buffer_.resize(estimated_size);
        
        char* dst_ptr = &conversion_buffer_[0];
        size_t dst_left = estimated_size - 1;
        const char* src_ptr = src;
        size_t src_left = src_len;
        
        // Don't reset converter state for each string - batch processing handles this
        size_t status = iconv(converter_, const_cast<char**>(&src_ptr), &src_left, &dst_ptr, &dst_left);
        
        if (status == static_cast<size_t>(-1)) {
            if (errno == E2BIG) {
                // Buffer too small - retry with larger buffer
                size_t larger_size = estimated_size * 2;
                conversion_buffer_.resize(larger_size);
                return convert_string_retry(dst, src, src_len, larger_size);
            } else if (errno == EILSEQ) {
                return CONVERT_BAD_STRING;
            } else if (errno != EINVAL) { // EINVAL indicates improper truncation; accept it
                return CONVERT_ERROR;
            }
        }
        
        // Efficient assignment using move semantics
        size_t converted_len = estimated_size - dst_left - 1;
        conversion_buffer_.resize(converted_len);
        dst = std::move(conversion_buffer_);
        
        // Prepare buffer for next use
        conversion_buffer_.clear();
        
        return CONVERT_OK;
    }
    
    convert_result convert_string_retry(std::string& dst, const char* src, size_t src_len, size_t buffer_size) const {
        conversion_buffer_.resize(buffer_size);
        
        char* dst_ptr = &conversion_buffer_[0];
        size_t dst_left = buffer_size - 1;
        const char* src_ptr = src;
        size_t src_left = src_len;
        
        // Reset state for retry
        iconv(converter_, nullptr, nullptr, nullptr, nullptr);
        
        size_t status = iconv(converter_, const_cast<char**>(&src_ptr), &src_left, &dst_ptr, &dst_left);
        
        if (status == static_cast<size_t>(-1)) {
            if (errno == E2BIG) {
                return CONVERT_LONG_STRING;
            } else if (errno == EILSEQ) {
                return CONVERT_BAD_STRING;
            } else if (errno != EINVAL) {
                return CONVERT_ERROR;
            }
        }
        
        size_t converted_len = buffer_size - dst_left - 1;
        conversion_buffer_.resize(converted_len);
        dst = std::move(conversion_buffer_);
        conversion_buffer_.clear();
        
        return CONVERT_OK;
    }
    
public:
    // For compatibility with existing code (single string conversion)
    convert_result convert_string(std::string& dst, const char* src, size_t src_len) const {
        return convert_string_optimized(dst, src, src_len);
    }
    
    bool needs_conversion() const {
        return is_valid_;
    }
    
    const std::string& get_source_encoding() const {
        return from_encoding_;
    }
    
    const std::string& get_target_encoding() const {
        return to_encoding_;
    }
};

class arrow_sink {
private:
    COLUMNS columns;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders_;
    int64_t chunk_size_;
    int64_t current_row_count_; // Tracks rows in the current, in-progress chunk
    bool builders_need_reset_ = false; 
    charset_converter converter_;
    std::vector<size_t> string_column_indices_; // Track which columns are strings
    
    // Convert SAS column type to Arrow DataType
    std::shared_ptr<arrow::DataType> sas_to_arrow_type(cppsas7bdat::Column::Type type) {
        switch (type) {
            case cppsas7bdat::Column::Type::string:
                return arrow::utf8();
            case cppsas7bdat::Column::Type::integer:
                return arrow::int64();
            case cppsas7bdat::Column::Type::number:
                return arrow::float64();
            case cppsas7bdat::Column::Type::datetime:
                return arrow::timestamp(arrow::TimeUnit::MICRO); // microsecond precision
            case cppsas7bdat::Column::Type::date:
                return arrow::date32(); // days since epoch
            case cppsas7bdat::Column::Type::time:
                return arrow::time64(arrow::TimeUnit::MICRO); // microseconds since midnight
            case cppsas7bdat::Column::Type::unknown:
            default:
                return arrow::utf8(); // fallback to string for unknown types
        }
    }
    
    // Create appropriate array builder for the column type
    std::shared_ptr<arrow::ArrayBuilder> create_builder(cppsas7bdat::Column::Type type) {
        auto pool = arrow::default_memory_pool();
        
        switch (type) {
            case cppsas7bdat::Column::Type::string: {
                auto builder = std::make_shared<arrow::StringBuilder>(pool);
                
                // Explicitly ignore the status with void cast
                (void)builder->Reserve(chunk_size_);
                (void)builder->ReserveData(chunk_size_ * 20);
                
                return builder;
            }
            case cppsas7bdat::Column::Type::integer: {
                auto builder = std::make_shared<arrow::Int64Builder>(pool);
                (void)builder->Reserve(chunk_size_);
                return builder;
            }
            case cppsas7bdat::Column::Type::number: {
                auto builder = std::make_shared<arrow::DoubleBuilder>(pool);
                (void)builder->Reserve(chunk_size_);
                return builder;
            }
            case cppsas7bdat::Column::Type::datetime: {
                auto builder = std::make_shared<arrow::TimestampBuilder>(
                    arrow::timestamp(arrow::TimeUnit::MICRO), pool);
                (void)builder->Reserve(chunk_size_);
                return builder;
            }
            case cppsas7bdat::Column::Type::date: {
                auto builder = std::make_shared<arrow::Date32Builder>(pool);
                (void)builder->Reserve(chunk_size_);
                return builder;
            }
            case cppsas7bdat::Column::Type::time: {
                auto builder = std::make_shared<arrow::Time64Builder>(
                    arrow::time64(arrow::TimeUnit::MICRO), pool);
                (void)builder->Reserve(chunk_size_);
                return builder;
            }
            case cppsas7bdat::Column::Type::unknown:
            default: {
                auto builder = std::make_shared<arrow::StringBuilder>(pool);
                (void)builder->Reserve(chunk_size_);
                (void)builder->ReserveData(chunk_size_ * 20);
                return builder;
            }
        }
    }
    
    // Append value to the appropriate builder (no encoding conversion here)
    arrow::Status append_value(size_t col_idx, Column::PBUF p) {
        const auto& column = columns[col_idx];
        auto& builder = builders_[col_idx];
        
        switch (column.type) {
            case cppsas7bdat::Column::Type::string: {
                auto string_builder = static_cast<arrow::StringBuilder*>(builder.get());
                auto value = column.get_string(p);
                // Store raw string data - conversion happens at batch finalization
                return string_builder->Append(std::string(value));
            }
            case cppsas7bdat::Column::Type::integer: {
                auto int_builder = static_cast<arrow::Int64Builder*>(builder.get());
                auto value = column.get_integer(p);
                return int_builder->Append(static_cast<int64_t>(value));
            }
            case cppsas7bdat::Column::Type::number: {
                auto double_builder = static_cast<arrow::DoubleBuilder*>(builder.get());
                auto value = column.get_number(p);
                if (std::isnan(value)) {
                    return double_builder->AppendNull();
                } else {
                    return double_builder->Append(value);
                }
            }
            case cppsas7bdat::Column::Type::datetime: {
                auto ts_builder = static_cast<arrow::TimestampBuilder*>(builder.get());
                auto value = column.get_datetime(p);
                if (value.is_not_a_date_time()) {
                    return ts_builder->AppendNull();
                } else {
                    auto epoch = boost::posix_time::ptime(boost::gregorian::date(1970, 1, 1));
                    auto duration = value - epoch;
                    int64_t microseconds = duration.total_microseconds();
                    return ts_builder->Append(microseconds);
                }
            }
            case cppsas7bdat::Column::Type::date: {
                auto date_builder = static_cast<arrow::Date32Builder*>(builder.get());
                auto value = column.get_date(p);
                if (value.is_not_a_date()) {
                    return date_builder->AppendNull();
                } else {
                    boost::gregorian::date epoch(1970, 1, 1);
                    auto days = (value - epoch).days();
                    return date_builder->Append(static_cast<int32_t>(days));
                }
            }
            case cppsas7bdat::Column::Type::time: {
                auto time_builder = static_cast<arrow::Time64Builder*>(builder.get());
                auto value = column.get_time(p);
                if (value.is_not_a_date_time()) {
                    return time_builder->AppendNull();
                } else {
                    int64_t microseconds = value.total_microseconds();
                    return time_builder->Append(microseconds);
                }
            }
            case cppsas7bdat::Column::Type::unknown:
            default: {
                auto string_builder = static_cast<arrow::StringBuilder*>(builder.get());
                auto value = column.to_string(p);
                // Store raw string data - conversion happens at batch finalization
                return string_builder->Append(std::string(value));
            }
        }
        return arrow::Status::OK();
    }
    
    // Finalize current chunk and create a record batch with encoding conversion if needed
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> finalize_current_chunk() {
        if (current_row_count_ == 0) {
            return arrow::Result<std::shared_ptr<arrow::RecordBatch>>(nullptr);
        }

        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(builders_.size());

        // First, finish all builders to get raw arrays
        for (auto& builder : builders_) {
            std::shared_ptr<arrow::Array> array;
            ARROW_RETURN_NOT_OK(builder->Finish(&array));
            arrays.push_back(array);
        }

        // Apply encoding conversion to string columns if needed
        if (converter_.needs_conversion() && !string_column_indices_.empty()) {
            for (size_t idx : string_column_indices_) {
                auto converted_result = converter_.convert_string_array_batch(arrays[idx]);
                ARROW_RETURN_NOT_OK(converted_result.status());
                arrays[idx] = converted_result.ValueOrDie();
            }
        }

        auto batch = arrow::RecordBatch::Make(schema_, current_row_count_, arrays);

        // DON'T reset builders immediately - defer until next push_row
        builders_need_reset_ = true;
        current_row_count_ = 0;

        return batch;
    }

public:
    explicit arrow_sink(int64_t chunk_size = 65536, const std::string& source_encoding = "") noexcept 
        : chunk_size_(chunk_size), current_row_count_(0), builders_need_reset_(false) {
        // Only set up converter if explicit encoding is provided
        if (!source_encoding.empty()) {
            converter_ = charset_converter(source_encoding, "UTF-8");
        }
    }

    ~arrow_sink() = default;
    
    void set_properties(const Properties& _properties) {
        columns = COLUMNS(_properties.columns);

        // Auto-detect encoding from SAS file if not already manually set
        if (!converter_.needs_conversion() && _properties.encoding != "UTF-8") {
            const std::string& detected_encoding = _properties.encoding;
            
            // Only set up conversion if it's not already UTF-8
            if (!detected_encoding.empty() && detected_encoding != "UTF-8") {
                converter_ = charset_converter(detected_encoding, "UTF-8");
            }
        }

        // Identify string column indices for batch conversion
        string_column_indices_.clear();
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].type == cppsas7bdat::Column::Type::string || 
                columns[i].type == cppsas7bdat::Column::Type::unknown) {
                string_column_indices_.push_back(i);
            }
        }

        // Create Arrow schema
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(columns.size());
        
        for (const auto& column : columns) {
            auto arrow_type = sas_to_arrow_type(column.type);
            fields.push_back(arrow::field(column.name, arrow_type));
        }
        
        schema_ = arrow::schema(fields);
        // Initialize builders
        builders_.clear();  // Clear any existing builders
        builders_.reserve(columns.size());
        
        for (size_t i = 0; i < columns.size(); ++i) {
            builders_.push_back(create_builder(columns[i].type));
        }
    }
    
    void set_column_names(const std::vector<std::string>&) noexcept {}
    void set_column_types(const std::vector<Column::Type>&) noexcept {}
    
    void push_row([[maybe_unused]] size_t irow, [[maybe_unused]] Column::PBUF p) {
        // Reset builders if they were finished in the last batch
        if (builders_need_reset_) {
            for (auto& builder : builders_) {
                builder->Reset();  // Reuse capacity instead of recreating
            }
            builders_need_reset_ = false;
        }
        
        // Process each column
        for (size_t i = 0; i < columns.size(); ++i) {
            auto status = append_value(i, p);
            if (!status.ok()) {
                printf("WARNING: Failed to append value for column %zu: %s\n", 
                    i, status.ToString().c_str());
                fflush(stdout);
                continue;
            }
        }
        
        current_row_count_++;
    }
    
    // This method is called by the cppsas7bdat::Reader when it finishes reading
    // its underlying data source. In a streaming context, its role is minimal
    // as the external FFI layer explicitly handles the final batch.
    void end_of_data() const noexcept {
        // No implicit batch finalization here.
        // All batching and finalization responsibility is with get_next_available_batch()
        // and get_final_batch() called by the FFI consumer.
    }
    
    // Returns the Arrow schema once it has been initialized by set_properties.
    std::shared_ptr<arrow::Schema> get_schema() const {
        return schema_;
    }
    
    // Returns the configured chunk size for this sink.
    int64_t get_chunk_size() const {
        return chunk_size_;
    }

    // Attempts to finalize and return a RecordBatch if enough rows (>= chunk_size)
    // have been accumulated. If not, returns nullptr (wrapped in Result).
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> get_next_available_batch() {
        if (current_row_count_ >= chunk_size_) {
            return finalize_current_chunk();
        }
        return arrow::Result<std::shared_ptr<arrow::RecordBatch>>(nullptr);
    }
    
    // Finalizes and returns any remaining partial RecordBatch at the end of data.
    // Returns nullptr if no rows are left.
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> get_final_batch() {
        if (current_row_count_ > 0) {
            return finalize_current_chunk();
        }
        return arrow::Result<std::shared_ptr<arrow::RecordBatch>>(nullptr);
    }
    
    // Set encoding after construction if needed
    void set_encoding(const std::string& source_encoding) {
        converter_ = charset_converter(source_encoding, "UTF-8");
    }
    
    // Get the current encoding being used for conversion
    std::string get_current_encoding() const {
        return converter_.needs_conversion() ? converter_.get_source_encoding() : "UTF-8";
    }
    
    // Check if encoding conversion is active
    bool is_converting_encoding() const {
        return converter_.needs_conversion();
    }
};

} // namespace detail

// The arrow_factory now supports specifying source encoding
struct arrow_factory {
    auto operator()(int64_t chunk_size = 65536, const std::string& source_encoding = "") const noexcept {
        return detail::arrow_sink(chunk_size, source_encoding);
    }
} arrow;

} // namespace datasink
} // namespace cppsas7bdat

#endif // _CPP_SAS7BDAT_SINK_ARROW_HPP_