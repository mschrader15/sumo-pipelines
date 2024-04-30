#include <cassert>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <utility>
#include <memory>

#include <arrow/io/file.h>
#include <arrow/util/config.h>

#include <parquet/api/reader.h>
#include <parquet/exception.h>
#include <parquet/stream_reader.h>
#include <parquet/stream_writer.h>

#define ARROW_WITH_ZSTD

#pragma once



std::shared_ptr<parquet::schema::GroupNode> GetFCDOutputSchema(
    bool include_leader = true,
    bool include_collision = true
) {
    parquet::schema::NodeVector fields;

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "id", parquet::Repetition::OPTIONAL, parquet::Type::BYTE_ARRAY,
        parquet::ConvertedType::UTF8));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "time", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
        parquet::ConvertedType::NONE));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "speed", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
        parquet::ConvertedType::NONE));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "accel", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
        parquet::ConvertedType::NONE));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "x", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
        parquet::ConvertedType::NONE));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "y", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
        parquet::ConvertedType::NONE));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "fuel", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
        parquet::ConvertedType::NONE));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "lane", parquet::Repetition::OPTIONAL, parquet::Type::BYTE_ARRAY,
        parquet::ConvertedType::UTF8));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "eclass", parquet::Repetition::OPTIONAL, parquet::Type::BYTE_ARRAY,
        parquet::ConvertedType::UTF8));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "time_loss", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
        parquet::ConvertedType::NONE));

    if (include_leader) {
        fields.push_back(parquet::schema::PrimitiveNode::Make(
            "leader_id", parquet::Repetition::OPTIONAL, parquet::Type::BYTE_ARRAY,
            parquet::ConvertedType::UTF8));

        fields.push_back(parquet::schema::PrimitiveNode::Make(
            "leader_distance", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE,
            parquet::ConvertedType::NONE));
    }

    if (include_collision) {
        fields.push_back(parquet::schema::PrimitiveNode::Make(
            "collision", parquet::Repetition::OPTIONAL, parquet::Type::BOOLEAN,
            parquet::ConvertedType::NONE));
    }

    return std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
}



class StreamWriter {
public:
    StreamWriter(std::string& outfile, bool include_leader = true, bool include_collision = true) {

        PARQUET_ASSIGN_OR_THROW(
            outfile_, arrow::io::FileOutputStream::Open(outfile));

        builder_.compression(parquet::Compression::ZSTD);
        // builder_.compression_level(15);

        include_leader_ = include_leader;
        include_collision_ = include_collision;

        // construct the writer_
        writer_ = parquet::StreamWriter{
            parquet::ParquetFileWriter::Open(outfile_, GetFCDOutputSchema(
                include_leader, include_collision
            ), builder_.build()) };

    };

    inline void writeRow(const std::string& id, const double time, bool collision) {
        const auto& pos = libsumo::Vehicle::getPosition(id);

        writer_ << id << time
            << libsumo::Vehicle::getSpeed(id)
            << libsumo::Vehicle::getAcceleration(id)
            << pos.x << pos.y
            << libsumo::Vehicle::getFuelConsumption(id)
            << libsumo::Vehicle::getLaneID(id)
            << libsumo::Vehicle::getEmissionClass(id)
            << libsumo::Vehicle::getTimeLoss(id);


        if (include_leader_) {
            const auto& leader = libsumo::Vehicle::getLeader(id, 1000);
            writer_ << leader.first << leader.second;
        }

        if (include_collision_) {
            writer_ << collision;
        }

        writer_ << parquet::EndRow;
    }

    void setRowGroupSize(int64_t size) {
        writer_.SetMaxRowGroupSize(size); // 1MB
    }

private:
    parquet::WriterProperties::Builder builder_;
    std::shared_ptr<arrow::io::FileOutputStream> outfile_;
    std::shared_ptr<parquet::schema::GroupNode> schema_;
    parquet::StreamWriter writer_;
    bool include_leader_;
    bool include_collision_;
};
