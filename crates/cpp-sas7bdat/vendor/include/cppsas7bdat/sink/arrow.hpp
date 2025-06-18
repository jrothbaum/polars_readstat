/**
 * \file include/cppsas7bdat/sink/arrow.hpp
 *
 * \brief Apache Arrow datasink for streaming chunked output
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
#include <memory>
#include <vector>
#include <string>

namespace cppsas7bdat {
namespace datasink {
namespace detail {

class arrow_sink {
private:
    COLUMNS columns;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders_;
    int64_t chunk_size_;
    int64_t current_row_count_; // Tracks rows in the current, in-progress chunk
    bool builders_need_reset_ = false; 
    
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
    
    // Append value to the appropriate builder
    arrow::Status append_value(size_t col_idx, Column::PBUF p) {
        const auto& column = columns[col_idx];
        auto& builder = builders_[col_idx];
        
        switch (column.type) {
            case cppsas7bdat::Column::Type::string: {
                auto string_builder = static_cast<arrow::StringBuilder*>(builder.get());
                auto value = column.get_string(p);
                return string_builder->Append(value);
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
                return string_builder->Append(value);
            }
        }
        return arrow::Status::OK();
    }
    
    // Finalize current chunk and create a record batch.
    // This method now returns the batch directly instead of storing it.
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> finalize_current_chunk() {
        if (current_row_count_ == 0) {
            return arrow::Result<std::shared_ptr<arrow::RecordBatch>>(nullptr);
        }

        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(builders_.size());

        for (auto& builder : builders_) {
            std::shared_ptr<arrow::Array> array;
            ARROW_RETURN_NOT_OK(builder->Finish(&array));
            arrays.push_back(array);
        }

        auto batch = arrow::RecordBatch::Make(schema_, current_row_count_, arrays);

        // DON'T reset builders immediately - defer until next push_row
        builders_need_reset_ = true;
        current_row_count_ = 0;

        return batch;
    }

public:
    explicit arrow_sink(int64_t chunk_size = 65536) noexcept 
        : chunk_size_(chunk_size), current_row_count_(0), builders_need_reset_(false) {
    }

    ~arrow_sink() {
    }
    
    void set_properties(const Properties& _properties) {
        columns = COLUMNS(_properties.columns);

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
};

} // namespace detail

// The arrow_factory now only supports the base arrow_sink.
struct arrow_factory {
    auto operator()(int64_t chunk_size = 65536) const noexcept {
        return detail::arrow_sink(chunk_size);
    }
} arrow;

} // namespace datasink
} // namespace cppsas7bdat

#endif // _CPP_SAS7BDAT_SINK_ARROW_HPP_
