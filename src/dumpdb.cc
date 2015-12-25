#include <string>
#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/stat.h>

#include "sdk/tera.h"
#include "sdk/table_impl.h"
#include "sdk/client_impl.h"
#include "sdk/sdk_utils.h"

#include "proto/dumpdb.pb.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

DECLARE_string(flagfile);
DECLARE_string(tera_zk_addr_list);
DECLARE_string(tera_zk_root_path);

using namespace tera;

void Usage() {
    std::cout << "\n\
        dump <tablename> \n\
        restore <tablename> \n" 
        << std::endl;
}

int main(int argc, char* argv[])
{
    if (argc < 2) {
        Usage();
        return -1;
    }
    int ret = 0;
    ErrorCode error_code;
    std::string tablename = argv[2];
    Client* client = Client::NewClient(FLAGS_flagfile, NULL);
    if (strcmp(argv[1], "dump") == 0) {
        if (argc != 3) {
            Usage();
            return -1;
        }
        //dump schema first 
        TableMeta table_meta;
        TabletMetaList tablet_list;

        tera::ClientImpl* client_impl = static_cast<tera::ClientImpl*>(client);
        if (!client_impl->ShowTablesInfo(tablename, &table_meta, &tablet_list, &error_code)) {
            std::cout << "Get table meta error." << std::endl;
            return -1;
        }
        std::string schema_str = GetTableSchema(table_meta.schema(), false);
        if (table_meta.schema().kv_only()) {
            schema_str.resize(schema_str.size() - 12);
        }
        mkdir("./dump_data", 0777);
        mkdir(std::string("./dump_data/" + tablename).c_str(), 0777);
        std::string prefix = "./dump_data/" + tablename + "/";
        std::ofstream  schema_s(std::string(prefix + "schema").c_str());
        schema_s << schema_str;
        schema_s.close();

        Table* table = NULL;
        if ((table = client->OpenTable(tablename, &error_code)) == NULL) {
            std::cout << "Open table: " << tablename << " error." << std::endl;
            return -1;
        }

        std::string start_rowkey = "";
        std::string end_rowkey = "";
        ResultStream* result_stream;
        ScanDescriptor desc(start_rowkey);
        desc.SetEnd(end_rowkey);
        desc.SetBufferSize(10240);
        desc.SetAsync(false);
        desc.SetMaxVersions(std::numeric_limits<int>::max());
        if ((result_stream = table->Scan(desc, &error_code)) == NULL) {
            std::cout << "Scan src table error" << std::endl;
            return -1;
        }
        std::fstream data_s(std::string(prefix + "data").c_str(),
                std::ios::out | std::ios::trunc | std::ios::binary);
        google::protobuf::io::ZeroCopyOutputStream *raw_out =
            new google::protobuf::io::OstreamOutputStream(&data_s);
        google::protobuf::io::CodedOutputStream *coded_out =
            new google::protobuf::io::CodedOutputStream(raw_out);
        while (!result_stream->Done()) {
            /*
            std::cout << result_stream->RowName() << ":"
                << result_stream->ColumnName() << ":"
                << result_stream->Timestamp() << ":"
                << result_stream->Value() << std::endl;
            */
            tera::DumpRecord record;
            std::string columnfamily = result_stream->ColumnName();
            if (columnfamily.find(":") != std::string::npos) {
                columnfamily.resize(columnfamily.find(":"));
            }
            record.set_rowname(result_stream->RowName());
            record.set_columnfamily(columnfamily);
            record.set_qualifier(result_stream->Qualifier());
            record.set_timestamp(result_stream->Timestamp());
            record.set_value(result_stream->Value());
            std::string infobuf;
            record.SerializeToString(&infobuf);
            coded_out->WriteVarint32(infobuf.size());
            coded_out->WriteRaw(infobuf.data(), infobuf.size());
            result_stream->Next();
        }
        std::cout << "dump table " << tablename << " successfully" << std::endl;
        delete table;
        delete coded_out;
        delete raw_out;
        data_s.close();
    } else if (strcmp(argv[1], "restore") == 0) {
        if (argc != 3) {
            Usage();
            return -1;
        }
        std::string prefix = "./dump_data/" + tablename + "/";
        std::string schema_file = prefix + "schema";
        TableDescriptor table_desc;
        if (!ParseTableSchemaFile(schema_file, &table_desc)) {
            std::cout << "Parse schema error" << std::endl;
            return -1;
        }
        std::vector<std::string> delimiters;
        /*
        std::string delimiter_file = prefix + "/delimitr";
        if (!ParseDelimiterFile(delimiter_file, &delimiters)) {
            std::cout << "Parse delimiter_file error" << std::endl;
            return -1;
        }
        */
        if (!client->CreateTable(table_desc, delimiters, &error_code)) {
            std::cout << "Create table error" << std::endl;
            return -1;
        }

        //restore records
        Table* table = NULL;
        if ((table = client->OpenTable(tablename, &error_code)) == NULL) {
            std::cout << "Open dest table error" << std::endl;
            return -1;
        }
        std::fstream data_s(std::string(prefix + "data").c_str(),
                std::ios::in | std::ios::binary);
        if (!data_s) {
            std::cout << "Open binary data error" << std::endl;
            return -1;
        }
        google::protobuf::io::ZeroCopyInputStream *raw_in =
            new google::protobuf::io::IstreamInputStream(&data_s);
        google::protobuf::io::CodedInputStream *coded_in =
            new google::protobuf::io::CodedInputStream(raw_in);

        uint32_t message_size = 0;
        while (coded_in->ReadVarint32(&message_size)) {
            std::string message_buf;
            coded_in->ReadString(&message_buf, message_size);
            tera::DumpRecord record;
            record.ParseFromString(message_buf);
            std::string columnfamily = "";
            std::string qualifier = "";
            if (record.has_columnfamily()) {
                columnfamily = record.columnfamily();
                qualifier = record.has_qualifier() ? record.qualifier() : "";
            }
            if (!table->Put(record.rowname(), columnfamily, qualifier, record.value(), &error_code)) {
                return -1;
            }
        }

        std::cout << "restore table " << tablename << " successfully" << std::endl;
        delete table;
        delete coded_in;
        delete raw_in;
        data_s.close();
    } else {
        Usage();
        ret = -1;
    }
    delete client;
    return ret;
}
